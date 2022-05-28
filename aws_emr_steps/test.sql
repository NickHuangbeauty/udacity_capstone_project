WITH t1 AS \
    (SELECT * \
       FROM immigration_main_information_data imid \
      LEFT JOIN imm_personal ip \
             ON imid.imm_main_cic_id = ip.imm_per_cic_id \
        WHERE imid.imm_year = 2016 \
     ), t2 AS \
        (SELECT * \
           FROM t1 \
         LEFT JOIN news_article_data nad \
                ON t1.imm_arrival_date = nad.news_publish_time \
     )
     SELECT * \
       FROM t2 \
     LEFT JOIN (SELECT * FROM us_cities_demographics_data ucdd INNER JOIN imm_destination_city_data idcd ON ucdd.cidemo_state_code = idcd.value_of_alias_imm_destination_city) src \
            ON t2.imm_port = src.code_of_imm_destination_city \





    SELECT * \
      FROM t2 \
    LEFT JOIN(SELECT idcd.code_of_imm_destination_city \
                     idcd.value_of_imm_destination_city \
                FROM us_cities_demographics_data ucdd \
              INNER JOIN imm_destination_city_data idcd \
                      ON ucdd.cidemo_state_code = idcd.value_of_alias_imm_destination_city) src \
           ON t2.imm_port = src.code_of_imm_destination_city \