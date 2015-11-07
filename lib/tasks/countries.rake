desc 'Convert h_country to countries'

namespace :au do
  task :countries do
    pg_conn = PG.connect(
      dbname: ENV['PGSQL_DB_NAME'], user: ENV['PGSQL_DB_USER'],
      host: "localhost", password: ENV['PGSQL_DB_PASS']
    )

    my_conn = Mysql2::Client.new(
      database: ENV['MYSQL_DB_NAME'], username: ENV['MYSQL_DB_USER'],
      host: "localhost", password: ENV['MYSQL_DB_PASS']
    )

    country_inserts = my_conn.query('select * from h_country').map do |row|
      id = row['countryID']
      name = row['countryTxt'].gsub('&nbsp;', ' ').gsub('&Aring;','Å')
        .gsub('&ocirc;','ô').gsub('&#39;',"''").gsub('&eacute;','é')

      "insert into countries (id, name) values (#{id}, '#{name}');"
    end

    create_countries = %{
      DROP TABLE IF EXISTS countries;
      CREATE TABLE countries (
        id serial NOT NULL,
        name character varying(255),
        CONSTRAINT countries_pkey PRIMARY KEY (id)
      )
      WITH (
        OIDS=FALSE
      );
    }.squish

    countries_sql = create_countries + ' ' + country_inserts.join("\n")

    pg_conn.exec(countries_sql)
  end
end
