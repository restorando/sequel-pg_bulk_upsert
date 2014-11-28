# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'sequel-pg_bulk_upsert/version'

Gem::Specification.new do |gem|
  gem.name          = "sequel-pg_bulk_upsert"
  gem.version       = Sequel::PgBulkUpsert::VERSION
  gem.authors       = ["Juan Manuel Barreneche"]
  gem.email         = ["jbarreneche@restorando.com"]
  gem.description   = %q{Implementation `on_duplicate_key_update(*...).multi_insert(*...)` for postgresql}
  gem.summary       = %q{Implementation `on_duplicate_key_update(*...).multi_insert(*...)` for postgresql}
  gem.homepage      = "https://github.com/restorando/sequel-pg_bulk_upsert"

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.add_dependency "sequel", ">= 4.15.0"
  gem.add_dependency "pg"
  gem.add_development_dependency "minitest"
  gem.add_development_dependency "pry"
  gem.add_development_dependency "rake"
  gem.require_paths = ["lib"]
end
