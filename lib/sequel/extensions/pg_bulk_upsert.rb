require "sequel"

module Sequel
  Dataset::NON_SQL_OPTIONS << :on_duplicate_key_update

  module PgBulkUpsert
    def on_duplicate_key_update(*args)
      clone(on_duplicate_key_update: args)
    end

    # def import(columns, values, opts=OPTS)
    #   if @opts[:on_duplicate_key_update]
    #     _bulk_upsert_import(columns, values, opts)
    #   else
    #     super
    #   end
    # end

    def multi_insert_sql(columns, values)
      if duplicate_keys = @opts[:on_duplicate_key_update]
        from_table_name = @opts[:from].first # XXX How can we ensure theres only one?
        temp_table_name = create_temp_table_from_existing(from_table_name)
        upsert_on = @db.primary_key(from_table_name).to_sym

        sqls = clone(on_duplicate_key_update: nil).from(temp_table_name).multi_insert_sql(columns, values)
        sqls << upsert_from_to_sql(temp_table_name, from_table_name, upsert_on, duplicate_keys, columns)
      else
        super
      end
    end

    private

    def upsert_from_to_sql(source_name, target_name, join_on, update_columns, insert_columns)
      target = @db[target_name]
      source = @db[source_name]
      source_ds = source.
        select(*insert_columns).
        left_join(:update_cte, [join_on]).
        where(Sequel.qualify(:update_cte, join_on) => nil)

      update_hash = update_columns.each_with_object({}) do |column, hash|
        hash[column] = Sequel.qualify(source_name, column)
      end

      target.with(:update_cte,
        target.from(target_name, source_name).
          where(Sequel.qualify(target_name, join_on) => Sequel.qualify(source_name, join_on)).
          returning(Sequel.qualify(target_name, join_on)).with_sql(:update_sql, update_hash)).
        returning(Sequel.qualify(target_name, join_on)).insert_sql(source_ds)
    end

    def create_temp_table_from_existing(base_table)
      temp_table_name = :ble

      columns_information = @db.schema(base_table)

      @db.create_table(temp_table_name, temp: true) do
        columns_information.each do |col, data|
          column col, data[:db_type], primary_key: data[:primary]
        end
      end

      temp_table_name
    end

  end

  module PgDatabaseBulkUpsert
    def self.extended(db)
      db.extend_datasets(PgBulkUpsert)
    end
  end

  Dataset.register_extension :pg_bulk_upsert, PgBulkUpsert
  Database.register_extension :pg_bulk_upsert, PgDatabaseBulkUpsert
end

