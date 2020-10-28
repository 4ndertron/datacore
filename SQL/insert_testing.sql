insert into wp_liftenergypitt.wp_posts ( post_author
                                       , post_date
                                       , post_date_gmt
                                       , post_content
                                       , post_title
                                       , post_excerpt
                                       , post_status
                                       , comment_status
                                       , ping_status
                                       , post_password
                                       , post_name
                                       , to_ping
                                       , pinged
                                       , post_modified
                                       , post_modified_gmt
                                       , post_content_filtered
                                       , post_parent
                                       , guid
                                       , menu_order
                                       , post_type
                                       , post_mime_type
                                       , comment_count)
values ( 24 -- post_author
       , current_timestamp -- post_date
       , current_timestamp -- post_date_gmt
       , '' -- post_content
       , (select i.idcount
          from (select max(cast(post_title as decimal)) + 1 as idcount
                from wp_liftenergypitt.wp_posts
                where post_type = 'account') as i) -- post_title
       , '' -- post_excerpt
       , 'publish' -- post_status
       , 'closed' -- comment_status
       , 'closed' -- ping_status
       , '' -- post_password
       , (select i.idcount
          from (select max(cast(post_title as decimal)) + 1 as idcount
                from wp_liftenergypitt.wp_posts
                where post_type = 'account') as i) -- post_name
       , '' -- to_ping
       , '' -- pinged
       , current_timestamp -- post_modified
       , current_timestamp -- post_modified_gmt
       , '' -- post_content_filtered
       , 0 -- post_parent
       , concat('https://thepitt.io/account/',
                (select i.idcount
                 from (select max(cast(post_title as decimal)) + 1 as idcount
                       from wp_liftenergypitt.wp_posts
                       where post_type = 'account') as i),
                '/') -- guid
       , 0 -- menu_order
       , 'account' -- post_type
       , '' -- post_mime_type
       , 0 -- comment_count
       );

insert into wp_liftenergypitt.wp_postmeta (post_id,
                                           meta_key,
                                           meta_value)
values ( (select max(ID)
          from wp_liftenergypitt.wp_posts
          where post_title =
                (select max(cast(post_title as decimal)) from wp_liftenergypitt.wp_posts where post_type = 'account'))
       , 'tp_location'
       , '123 street, city, state, zip, country'),
       ( (select max(ID)
          from wp_liftenergypitt.wp_posts
          where post_title =
                (select max(cast(post_title as decimal)) from wp_liftenergypitt.wp_posts where post_type = 'account'))
       , '_tp_location'
       , 'field_5f5a4c70da8c1')
#        todo: convert this postmeta insert into a pivot insert so
#         the datahandler can melt the values into the postmeta table.
;