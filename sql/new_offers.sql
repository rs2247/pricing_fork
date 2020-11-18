select distinct
    universities.id university_id
    ,universities.name university_name
    ,count(distinct offers.id) n_offers
from offers
    join universities on universities.id=offers.university_id
    join courses on courses.id=offers.course_id
    join levels on levels.name=courses.level
    join kinds on kinds.name=courses.kind
    join levels plevel on plevel.id=levels.parent_id
    join kinds pkind on pkind.id=kinds.parent_id

    join product_lines_levels on product_lines_levels.level_id=plevel.id
    join product_lines_kinds on product_lines_kinds.kind_id=pkind.id
    join product_lines 
        on product_lines.id=product_lines_kinds.product_line_id
        and product_lines.id=product_lines_levels.product_line_id
    join university_quality_owners on university_quality_owners.university_id=universities.id
        and university_quality_owners.end_date is null
        and university_quality_owners.product_line_id=product_lines.id
    join admin_users quali on quali.id=university_quality_owners.admin_user_id
    join university_deal_owners on university_deal_owners.university_id=universities.id
        and university_deal_owners.end_date is null
        and university_deal_owners.product_line_id=product_lines.id
    join admin_users ka on ka.id=university_deal_owners.admin_user_id
where
1=1
and offers.enabled
and now()::date-offers.start::date<=7
and coalesce(offers.show_on_main_search,true)=true
and offers.status is null
and (quali.id={0} or ka.id={0})
group by 1,2
order by 3 desc 
