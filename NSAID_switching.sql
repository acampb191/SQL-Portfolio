/*Only chronic patients using NSAID named products, general NSAID excluded*/
with
chronic_nsaid_transactions as (
    select *
    from {{ref('syndicated_pain_all_pain_purchases')}}
    where
        pain_type = 'Chronic'
        and category_name = 'NSAIDs'
        and product_name not in ('Other NSAID', 'Other NSAID Inj')
)
, patient_data as (
    select
        pain_purchases.practice_odu_id
        , pain_purchases.region
        , pain_purchases.location_type
        , pain_purchases.is_corporate_group
        , pain_purchases.practice_size
        , pain_purchases.has_hd
        , pain_purchases.projection_practice
        , pain_purchases.patient_odu_id
        , pain_purchases.pain_type
        , pain_purchases.age_group
        , pain_purchases.species
        , pain_purchases.timeframe
        , pain_purchases.category_name
        , pain_purchases.product_name
        , pain_purchases.drug
        , pain_purchases.is_brand_name
        , pain_purchases.transaction_datetime
        , pain_purchases.revenue
        , listagg(distinct same_day_purchases.product_name, ', ') as same_day_products
        , max(previous_purchases.transaction_datetime) as previous_purchase_date
    from chronic_nsaid_transactions as pain_purchases
    /* Finds any Pain purchases that happened on the same day - Prevents combinations from being classified as a switch */
    left join chronic_nsaid_transactions as same_day_purchases
        on pain_purchases.practice_odu_id = same_day_purchases.practice_odu_id
        and pain_purchases.patient_odu_id = same_day_purchases.patient_odu_id
        and pain_purchases.transaction_datetime = same_day_purchases.transaction_datetime
        and replace(pain_purchases.product_name, ' Inj', '') != replace(same_day_purchases.product_name, ' Inj', '')
    /* Finds date of previous purchase */
    left join chronic_nsaid_transactions as previous_purchases
        on pain_purchases.practice_odu_id = previous_purchases.practice_odu_id
        and pain_purchases.patient_odu_id = previous_purchases.patient_odu_id
        and pain_purchases.transaction_datetime > previous_purchases.transaction_datetime
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 ,17, 18
)
, int_data as (
    select
        patient_data.practice_odu_id
        , patient_data.region
        , patient_data.location_type
        , patient_data.is_corporate_group
        , patient_data.practice_size
        , patient_data.has_hd
        , patient_data.projection_practice
        , patient_data.patient_odu_id
        , patient_data.pain_type
        , patient_data.age_group
        , patient_data.species
        , patient_data.timeframe
        , patient_data.category_name
        , patient_data.product_name
        , patient_data.drug
        , patient_data.is_brand_name
        , patient_data.transaction_datetime
        , patient_data.revenue
        , previous_purchases.drug as previous_drug
        /* if previous drug purchase doesn't match current purchase drug, is a switch */
        , max(case when previous_purchases.drug != patient_data.drug then true else false end) as switch_yn
        /*If previous drug purchase is the same as the current drug purchase, is a brand name/generic switch */
        , max(case when previous_purchases.drug = patient_data.drug
                    and previous_purchases.is_brand_name = true then true else false end) as brand_name_switch_yn
        , row_number() over (order by patient_data.patient_odu_id, patient_data.transaction_datetime) as rn
        /* Finds the date of the patients last pain purchase */
        , max(patient_data.transaction_datetime) over (partition by
                                                patient_data.patient_odu_id
                                                , patient_data.pain_type
                                                , patient_data.species
                                                , patient_data.category_name order by patient_data.transaction_datetime rows between unbounded preceding and 1 preceding) as previous_purchase
        /* Finds the date of the patients last, same product, pain purchase */
        , max(patient_data.transaction_datetime) over (partition by
                                                patient_data.patient_odu_id
                                                , patient_data.pain_type
                                                , patient_data.species
                                                , patient_data.product_name
                                                , patient_data.category_name order by patient_data.transaction_datetime rows between unbounded preceding and 1 preceding) as last_purchase_of_product
    from patient_data
    /* Only joins to products that DO NOT match the previous pain purchase */
    left join patient_data as previous_purchases
        on patient_data.practice_odu_id = previous_purchases.practice_odu_id
        and patient_data.patient_odu_id = previous_purchases.patient_odu_id
        and patient_data.previous_purchase_date = previous_purchases.transaction_datetime
        /* Not a purchase of the same product */
        and replace(patient_data.product_name, ' Inj', '') != replace(previous_purchases.product_name, ' Inj', '')
        /* Not a purchase of a product that was purchased on the same day as the product */
        and patient_data.same_day_products not ilike '%' || previous_purchases.product_name || '%'
        and previous_purchases.same_day_products not ilike '%' || patient_data.product_name || '%'
        and patient_data.product_name not in ('Other NSAID', 'Other NSAID Inj')
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 ,17, 18, 19
    order by patient_data.patient_odu_id, patient_data.transaction_datetime
)
, product_purchase_islands as (
    /* Finds the start of each product */
    select
        *
        , sum(case
                when previous_purchase is null /* No previous pain purchases */
                    or last_purchase_of_product is null /* Never purchased the product before */
                    or last_purchase_of_product != previous_purchase /* Previous purchase of the product did not happen on the previous pain purchase */
                then 1 else 0 end) over (order by rn) as island_id
    from int_data
)
, purchase_data as (
    /* Counts how many times a patient purchased the product in the window */
    select
        island_id
        , count(island_id) as previous_purchases
    from product_purchase_islands
    group by 1
)
select
    practice_odu_id
    , region
    , location_type
    , is_corporate_group
    , practice_size
    , has_hd
    , projection_practice
    , patient_odu_id
    , pain_type
    , age_group
    , species
    , timeframe
    , category_name
    , product_name
    , drug
    , is_brand_name
    , transaction_datetime
    , revenue
    , previous_drug
    , switch_yn
    , brand_name_switch_yn
    , purchase_data.previous_purchases
from product_purchase_islands
left join purchase_data
    /* Shows how many times a patient purchased a product before switching*/
    on product_purchase_islands.island_id = purchase_data.island_id+1
    and (switch_yn = true or brand_name_switch_yn = true)
order by patient_odu_id, transaction_datetime
