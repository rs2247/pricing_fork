create materialized view test.pricing_ies_tests as
    select distinct
        offers.university_id,
        pricing_offers.test_id,
        count(distinct offers.id) offers
    from test.pricing_offers
        join querobolsa_production.offers 
            on offers.id=pricing_offers.offer_id
    where
        pricing_offers.end_date is null
    group by 1,2;
    
create index on test.pricing_ies_tests (university_id);

refresh materialized view test.pricing_ies_tests;  
