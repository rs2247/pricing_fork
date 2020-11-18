select distinct
    offers.id offer_id
    ,case when '{18}'='frozen' then '{18}'
        when '{18}'='enabled' and offers.visible then 'visible' 
        else '{18}' end as status
    ,offers.visible
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
    {10}join university_quality_owners on university_quality_owners.university_id=universities.id
    {10}    and university_quality_owners.end_date is null
    {10}    and university_quality_owners.product_line_id=product_lines.id
where
    1=1
    and offers.enabled
    and coalesce(offers.status,'enabled')<>'{18}'
    {0}and offers.university_id={1}
    {2}and kinds.parent_id={3}
    {4}and levels.parent_id={5}
    {6}and courses.campus_id in ({7})
    {8}and offers.discount_percentage<={9}
    {10}and university_quality_owners.admin_user_id={11}
    {12}and offers.id in ({13})
    {14}and offers.visible={15}
    {16}and offers.discount_percentage-coalesce(offers.commercial_discount,0)<={17}
