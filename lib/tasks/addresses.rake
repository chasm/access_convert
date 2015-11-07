desc 'Convert addresse to addresses'

namespace :au do
  task :addresses do
    pg_conn = PG.connect(
      dbname: ENV['PGSQL_DB_NAME'], user: ENV['PGSQL_DB_USER'],
      host: "localhost", password: ENV['PGSQL_DB_PASS']
    )

    my_conn = Mysql2::Client.new(
      database: ENV['MYSQL_DB_NAME'], username: ENV['MYSQL_DB_USER'],
      host: "localhost", password: ENV['MYSQL_DB_PASS']
    )

    states = {
      1 => 'None', 2 => 'ACT', 3 => 'NSW', 4 => 'NT', 5 => 'QLD', 6 => 'SA',
      7 => 'TAS', 8 => 'VIC', 9 => 'WA'
    }

    address_inserts = my_conn.query('select * from addresse').map do |row|
      values = []

      values << row['brukerID']
      values << if row['address1'].blank? then 'NULL' else "'#{row['address1'].gsub("'", "''")}'" end
      values << if row['address2'].blank? then 'NULL' else "'#{row['address2'].gsub("'", "''")}'" end
      values << if row['city'].blank? then 'NULL' else "'#{row['city'].gsub("'", "''").upcase}'" end
      values << if states[row['stateID']].blank? then 'NULL' else "'#{states[row['stateID']].gsub("'", "''")}'" end
      values << if row['countryID'].blank? then 'NULL' else row['countryID'] end
      values << if row['pcode'].blank? then 'NULL' else "'#{row['pcode'].gsub("'", "''")}'" end
      values << if row['homephone'].blank? then 'NULL' else "'#{row['homephone'].gsub("'", "''")}'" end
      values << if row['workphone'].blank? then 'NULL' else "'#{row['workphone'].gsub("'", "''")}'" end
      values << if row['mobile'].blank? then 'NULL' else "'#{row['mobile'].gsub("'", "''")}'" end
      values << if row['membergrouptypeID'].blank? then 'NULL' else row['membergrouptypeID'] end
      values << if row['hearaboutaccessID'].blank? then 'NULL' else row['hearaboutaccessID'] end
      values << if row['hearaboutaccessother'].blank? then 'NULL' else "'#{row['hearaboutaccessother'].gsub("'", "''")}'" end

      %{
        insert into addresses (
          user_id,
          address_line_1,
          address_line_2,
          city,
          state,
          country_id,
          postal_code,
          home_phone,
          work_phone,
          mobile_phone,
          member_group_id,
          how_heard_about_id,
          how_heard_other
        ) values (#{values.join(', ')});
      }.squish
    end

    create_addresses = %{
      DROP TABLE IF EXISTS addresses;
      CREATE TABLE addresses (
        id serial NOT NULL,
        user_id integer,
        address_line_1 character varying(255),
        address_line_2 character varying(255),
        city character varying(255),
        state character varying(255),
        country_id integer,
        postal_code character varying(255),
        home_phone character varying(255),
        work_phone character varying(255),
        mobile_phone character varying(255),
        member_group_id integer,
        how_heard_about_id integer,
        how_heard_other character varying(255),
        CONSTRAINT addresses_pkey PRIMARY KEY (id)
      )
      WITH (
        OIDS=FALSE
      );
    }.squish

    addresses_sql = create_addresses + ' ' + address_inserts.join("\n")

    pg_conn.exec(addresses_sql)
  end
end
