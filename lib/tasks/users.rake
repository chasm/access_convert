desc 'Convert brukere to users'

namespace :au do
  task :users do
    pg_conn = PG.connect(
      dbname: ENV['PGSQL_DB_NAME'], user: ENV['PGSQL_DB_USER'],
      host: "localhost", password: ENV['PGSQL_DB_PASS']
    )

    my_conn = Mysql2::Client.new(
      database: ENV['MYSQL_DB_NAME'], username: ENV['MYSQL_DB_USER'],
      host: "localhost", password: ENV['MYSQL_DB_PASS']
    )

    salutations = {
      1 => 'None', 2 => 'Dr.', 3 => 'Miss', 4 => 'Mr.', 5 => 'Mrs.', 6 => 'Ms.'
    }

    user_inserts = my_conn.query('select * from brukere').map do |row|
      values = []

      values << row['brukerID']
      values << if salutations[row['salutationID']].blank? then 'NULL' else "'#{salutations[row['salutationID']]}'" end
      values << if row['brukernavn'].blank? then 'NULL' else "'#{row['brukernavn'].gsub("'", "''")}'" end
      values << if row['fornavn'].blank? then 'NULL' else "'#{row['fornavn'].gsub("'", "''")}'" end
      values << if row['etternavn'].blank? then 'NULL' else "'#{row['etternavn'].gsub("'", "''")}'" end
      values << if row['firma'].blank? then 'NULL' else "'#{row['firma'].gsub("'", "''")}'" end
      values << if row['epost'].blank? then 'NULL' else "'#{row['epost'].gsub("'", "")}'" end
      values << if row['passord'].blank? then 'NULL' else "'#{row['passord']}'" end
      values << if row['forgothash'].blank? then 'NULL' else "'#{row['forgothash']}'" end
      values << if row['startdato'].blank? then 'NULL' else "'#{row['startdato']}'" end
      values << if row['fornyetdato'].blank? then 'NULL' else "'#{row['fornyetdato']}'" end
      values << if row['brukernivaID'].blank? then 'NULL' else row['brukernivaID'] end
      values << if row['sperretID'].blank? then 'NULL' else row['sperretID'] end
      values << if row['nyhetsbrevID'].blank? then 'NULL' else row['nyhetsbrevID'] end

      %{
        insert into users (
          id,
          salutation,
          username,
          given_name,
          family_name,
          company,
          email,
          password_digest,
          password_reset_token,
          started_on,
          renewed_on,
          user_level_id,
          banned_id,
          newsletter_id
        ) values (#{values.join(', ')});
      }.squish
    end

    create_users = %{
      DROP TABLE IF EXISTS users;
      CREATE TABLE users (
        id serial NOT NULL,
        salutation character varying(255),
        username character varying(255),
        given_name character varying(255),
        family_name character varying(255),
        company character varying(255),
        email character varying(255),
        password_digest character varying(255),
        password_reset_token character varying(255),
        started_on date,
        renewed_on date,
        user_level_id integer,
        banned_id integer,
        newsletter_id integer,
        CONSTRAINT users_pkey PRIMARY KEY (id)
      )
      WITH (
        OIDS=FALSE
      );
    }.squish

    users_sql = create_users + ' ' + user_inserts.join("\n")

    pg_conn.exec(users_sql)
  end
end
