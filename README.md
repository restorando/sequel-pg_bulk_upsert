# Sequel::PgBulkUpsert

This Sequel extension implements `on_duplicate_key_update(*...).multi_insert(*...)` for postgresql. The
syntax is 100% with the one that Sequel provides for mysql with a couple of caveats.

There are different ways for implementing "upsert" in postgresql, some of them discussed in:

http://johtopg.blogspot.com.ar/2014/04/upsertisms-in-postgres.html

Because we aim to do bulk upserts, this extension uses the one discribed here:

http://tapoueh.org/blog/2013/03/15-batch-update

It consists in 3 parts:

1. Create a temp table with the target table structure

2. Fill the temp table with the rows that are going to be "upserted"

3. Updates the records that exist in the target table with the temp table information, then, by doing an
   "anti-join" between the temp table and the target table, the records that didn't exist are inserted.

   The resulting SQL looks like this:

   ```SQL
      WITH "update_cte" AS
        (UPDATE "<target_table>"
            SET "updatable_column" = "<temp_table>"."updatable_column"
           FROM "<temp_table>"
           WHERE ("<target_table>"."id" = "<temp_table>"."id")
           RETURNING "<target_table>"."id")
      INSERT INTO "<target_table>" ("updatable_column", "insertable_column")
       SELECT "updatable_column", "insertable_column"
       FROM "<temp_table>" LEFT JOIN "update_cte" USING ("id")
       WHERE ("update_cte"."id" IS NULL)
       RETURNING "<target_table>"."id"
   ```

## Caveats

1. The target table **must** have a primary key (this is the key used to decide when a record "exist"), it
   won't work for any other unique constraint in the target table.

2. This strategy for upsert isn't suitable for concurrent upserts on the same table. Because the concurrent
  inserts will see the same table, and neither will do an update on the others records, which will result in
  a duplicate key error.


## Installation

Add this line to your application's Gemfile:

    gem 'sequel-pg_bulk_upsert'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install sequel-pg_bulk_upsert

## Usage

```ruby
require 'sequel/pg_bulk_upsert'

# This can also be done for a specify dataset
DB.extension(:pg_bulk_upsert)

DB[:target].on_duplicate_key_update(:column1, :column2).multi_insert([
  {column1: '1', column2: '2', column3: '3'},
  {column1: '4', column2: '4', column3: '4'}
])
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
