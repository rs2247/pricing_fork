select distinct
    universities.id university_id,
    unaccent(lower(universities.name)) university_name,
    unaccent(lower(admin_users.name)) as "owner",
    university_deal_owners.product_line_id pl
from universities
    join university_deal_owners
        on university_deal_owners.university_id=universities.id
        and university_deal_owners.end_date is null
    join admin_users on admin_users.id=university_deal_owners.admin_user_id
where
    1=1
    and unaccent(lower(universities.name)) like '%{0}%'
    --and universities.status='partner'
order by 2
limit 8
