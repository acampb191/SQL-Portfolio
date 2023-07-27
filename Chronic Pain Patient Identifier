with
int_pain_transactions as (
    select distinct
        transactions.server_odu_id
        , transactions.practice_odu_id
        , transactions.patient_odu_id
        , patients.species
        , patients.birth_date
        , transactions.transaction_datetime
        , greatest(transactions.odu_updated_at_utc, coalesce(catalog_tag_mappings.odu_updated_at_utc, transactions.odu_updated_at_utc)) as odu_updated_at_utc
        , transactions.invoice_odu_id
        , transactions.transaction_odu_id
        , case
            when catalog_tag_mappings.catalog_tag_odu_id in (56, 60) then catalog_tag_mappings.catalog_tag_odu_id /* Joint Diet and Urinalysis */
            when transactions.revenue_category_odu_id in {{anesthesia_hospitalization_revenue_categories()}}
                or transactions.revenue_category_odu_id = 7 /* Exams */
                or transactions.revenue_category_odu_id in {{analgesic_tag_revenue_categories()}}
                or transactions.revenue_category_odu_id in {{invalid_pain_revenue_categories()}}
                or transactions.revenue_category_odu_id in {{alternative_therapy_revenue_categories()}}
                or transactions.revenue_category_odu_id in {{anti_inflammatory_revenue_categories()}}
                or transactions.revenue_category_odu_id in {{antihistamine_revenue_categories()}}
                or transactions.revenue_category_odu_id in {{immunotherapy_revenue_categories()}}
                or transactions.revenue_category_odu_id in {{skin_topical_revenue_categories()}}
                or transactions.revenue_category_odu_id in {{opiate_revenue_categories()}}
                or transactions.revenue_category_odu_id = 285 /* Supplements */
                or transactions.revenue_category_odu_id in {{derm_steroid_revenue_categories()}}
                or transactions.revenue_category_odu_id in {{mab_revenue_categories()}}
                or transactions.revenue_category_odu_id in {{gabapentin_tramadol_revenue_categories()}}
                then transactions.revenue_category_odu_id
            when transactions.level_3_revenue_category_id in {{nsaid_revenue_categories()}} then transactions.level_3_revenue_category_id
            when transactions.level_2_revenue_category_id in {{antibiotic_revenue_categories()}}
                or transactions.level_2_revenue_category_id = 251 /* DMOAD */
                or transactions.level_2_revenue_category_id in {{steroid_revenue_categories()}}
                then transactions.level_2_revenue_category_id
            when transactions.top_revenue_category_id in {{dental_and_surgery_revenue_categories()}}
                or transactions.top_revenue_category_id = 16 then transactions.top_revenue_category_id
        end as tag
    from {{ref('transactions')}} as transactions
    inner join {{ref('patients')}} as patients
        on transactions.server_odu_id = patients.server_odu_id
        and transactions.patient_odu_id = patients.patient_odu_id
    inner join {{ ref('syndicated_practice_info') }} as practice_whitelist
        on transactions.server_odu_id = practice_whitelist.server_odu_id
        and transactions.practice_odu_id = practice_whitelist.practice_odu_id
        and practice_whitelist.timeframe = 'Current Year'
        and practice_whitelist.patient_type = 'Non Home Delivery'
    left join {{ref('catalog_tag_mappings')}} as catalog_tag_mappings
        on transactions.catalog_source_server_odu_id = catalog_tag_mappings.catalog_source_server_odu_id
        and transactions.catalog_odu_id = catalog_tag_mappings.catalog_odu_id
        and catalog_tag_mappings.catalog_tag_odu_id in (56, 60)
    where
        transactions.transaction_datetime >= add_months(date_trunc('month', current_date), -49) /* Needs to be 13 months larger than window */
        and transactions.transaction_datetime < date_trunc('month', current_date)
        and (not ilike(transactions.description, '%declined%')
            or transactions.description is null)
        and patients.species in ('canine', 'feline')
        and transactions.is_revenue = true
        and (
            /* Pain Transactions */
            catalog_tag_mappings.catalog_tag_odu_id = 56 /* Joint Diet*/
            or transactions.level_3_revenue_category_id = 274 /* NSAID */
            or transactions.level_2_revenue_category_id = 251 /*DMOAD*/
            or transactions.revenue_category_odu_id = 78
            or transactions.revenue_category_odu_id in {{gabapentin_tramadol_revenue_categories()}}
            or transactions.revenue_category_odu_id = 285 /* Supplements */
            or transactions.revenue_category_odu_id in {{anti_inflammatory_revenue_categories()}}
            or transactions.revenue_category_odu_id in {{mab_revenue_categories()}}
            or transactions.revenue_category_odu_id in {{opiate_revenue_categories()}}
            or transactions.revenue_category_odu_id in {{alternative_therapy_revenue_categories()}}
            /* Invalid Transactions*/
            or transactions.top_revenue_category_id in {{dental_and_surgery_revenue_categories()}}
            or transactions.level_2_revenue_category_id in {{antibiotic_revenue_categories()}}
            or transactions.level_2_revenue_category_id in {{steroid_revenue_categories()}}
            or transactions.revenue_category_odu_id in {{anesthesia_hospitalization_revenue_categories()}}
            or transactions.revenue_category_odu_id in {{invalid_pain_revenue_categories()}}
            or transactions.revenue_category_odu_id in {{antihistamine_revenue_categories()}}
            or transactions.revenue_category_odu_id in {{skin_topical_revenue_categories()}}
            or transactions.revenue_category_odu_id in {{derm_steroid_revenue_categories()}}
            or transactions.revenue_category_odu_id = 7 /*Exams */
            or catalog_tag_mappings.catalog_tag_odu_id = 60 /* Urinalysis*/
            or transactions.top_revenue_category_id = 16 /* Vaccines */
            )
)
, invalid_invoice as (
    select
        transactions.server_odu_id
        , transactions.patient_odu_id
        , transactions.transaction_datetime
        , transactions.invoice_odu_id
        , transactions.transaction_odu_id
        , transactions.tag
    from int_pain_transactions as transactions
    where
        transactions.tag in {{anesthesia_hospitalization_revenue_categories()}}
        or transactions.tag in {{invalid_pain_revenue_categories()}}
        or transactions.tag in {{immunotherapy_revenue_categories()}}
        or transactions.tag = 16 /* Vaccines */
        or transactions.tag in (207, 208) /* Anticonvulsant */
        or transactions.tag in (209) /* Antidepressant */
        or transactions.tag in (260, 261) /* Sedatives */
)
, invalid_transactions as (
    select
        transactions.server_odu_id
        , transactions.patient_odu_id
        , transactions.transaction_datetime
        , transactions.invoice_odu_id
        , transactions.transaction_odu_id
        , transactions.tag
    from int_pain_transactions as transactions
    where
        transactions.tag in {{dental_and_surgery_revenue_categories()}}
        or transactions.tag = 7 /* Exams */
        or transactions.tag = 60 /* Urinalysis */
        or transactions.tag in {{antibiotic_revenue_categories()}}
        or transactions.tag in {{antihistamine_revenue_categories()}}
        or transactions.tag in {{skin_topical_revenue_categories()}}
        or transactions.tag in {{steroid_revenue_categories()}}
        or transactions.tag in {{derm_steroid_revenue_categories()}}
        or transactions.tag in {{invalid_pain_revenue_categories()}}
        /* These ones are validating transactions */
        or transactions.tag = 78 /* Analgesic */
        or transactions.tag in {{anti_inflammatory_revenue_categories()}}
        or transactions.tag = 251 /* DMOAD */
        or transactions.tag = 274 /* NSAIDs */
        or transactions.tag = 285 /* Supplements */
        or transactions.tag in {{mab_revenue_categories()}}
        or transactions.tag in {{gabapentin_tramadol_revenue_categories()}}
        or transactions.tag = 56 /* Joint Diet*/
)
, invalid_pain_transactions as (
    select distinct
        transactions.server_odu_id
        , transactions.patient_odu_id
        , transactions.transaction_odu_id
    from int_pain_transactions as transactions
    left join invalid_invoice as invalid_invoice
        on transactions.server_odu_id = invalid_invoice.server_odu_id
        and transactions.patient_odu_id = invalid_invoice.patient_odu_id
        and transactions.invoice_odu_id = invalid_invoice.invoice_odu_id
    left join invalid_transactions
        on transactions.server_odu_id = invalid_transactions.server_odu_id
        and transactions.patient_odu_id = invalid_transactions.patient_odu_id
        and abs(datediff(days, invalid_transactions.transaction_datetime, transactions.transaction_datetime)) <= 14
    where
        ((transactions.tag in {{nsaid_revenue_categories()}}
            or transactions.tag in {{anti_inflammatory_revenue_categories()}})
            and ((abs(datediff(day, invalid_transactions.transaction_datetime, transactions.transaction_datetime)) <= 7
                    and (invalid_transactions.tag in {{dental_and_surgery_revenue_categories()}}
                        or invalid_transactions.tag = 60
                        or invalid_transactions.tag in {{invalid_pain_revenue_categories()}}
                        or invalid_transactions.tag in {{antibiotic_revenue_categories()}}
                        or invalid_transactions.tag in {{antihistamine_revenue_categories()}}
                        or invalid_transactions.tag in {{skin_topical_revenue_categories()}}
                        or invalid_transactions.tag in {{steroid_revenue_categories()}}
                        or invalid_transactions.tag in {{derm_steroid_revenue_categories()}}
                        )
                    )
                or invalid_invoice.tag in {{anesthesia_hospitalization_revenue_categories()}}
                or invalid_invoice.tag = 16 /* Vaccines */
                )
        )

        or (transactions.tag = 285 /* Glucosamine + Chondroitin Supplements */
            and (invalid_invoice.tag in {{invalid_pain_revenue_categories()}}
                or invalid_invoice.tag in {{immunotherapy_revenue_categories()}})
            )

        or (transactions.tag = 78 /* Non-Injectable Analgesic */
            and ((abs(datediff(day, invalid_transactions.transaction_datetime, transactions.transaction_datetime)) <= 7
                    and (invalid_transactions.tag in {{dental_and_surgery_revenue_categories()}}
                        or invalid_transactions.tag = 60
                        or invalid_transactions.tag in {{invalid_pain_revenue_categories()}}
                        or invalid_transactions.tag in {{antibiotic_revenue_categories()}}
                        or invalid_transactions.tag in {{antihistamine_revenue_categories()}}
                        or invalid_transactions.tag in {{skin_topical_revenue_categories()}}
                        or invalid_transactions.tag in {{steroid_revenue_categories()}}
                        or invalid_transactions.tag in {{derm_steroid_revenue_categories()}}
                        )
                    )
                or (invalid_transactions.tag = 7 /* Exams */
                    and datediff(day, transactions.transaction_datetime, invalid_transactions.transaction_datetime) between 0 and 14)
                or invalid_invoice.tag in {{anesthesia_hospitalization_revenue_categories()}}
                or invalid_invoice.tag = 16
                )
        )

        or (transactions.tag in {{gabapentin_tramadol_revenue_categories()}}
            and ((abs(datediff(day, invalid_transactions.transaction_datetime, transactions.transaction_datetime)) <= 7
                    and (invalid_transactions.tag in {{dental_and_surgery_revenue_categories()}}
                        or invalid_transactions.tag = 60
                        or invalid_transactions.tag in {{invalid_pain_revenue_categories()}}
                        or invalid_transactions.tag in {{antibiotic_revenue_categories()}}
                        or invalid_transactions.tag in {{antihistamine_revenue_categories()}}
                        or invalid_transactions.tag in {{skin_topical_revenue_categories()}}
                        or invalid_transactions.tag in {{steroid_revenue_categories()}}
                        or invalid_transactions.tag in {{derm_steroid_revenue_categories()}}
                        )
                    )
                or (invalid_transactions.tag = 7 /* Exams */
                    and datediff(day, transactions.transaction_datetime, invalid_transactions.transaction_datetime) between 0 and 14)
                or invalid_invoice.tag in {{anesthesia_hospitalization_revenue_categories()}}
                or invalid_invoice.tag in (207, 208) /* Anticonvulsant */
                or invalid_invoice.tag in (209) /* Antidepressant */
                or invalid_invoice.tag in (260, 261) /* Sedatives */
                )
        )

        or (transactions.tag in {{opiate_revenue_categories()}}
            and (((invalid_transactions.tag in {{dental_and_surgery_revenue_categories()}}
                    or invalid_transactions.tag in {{antibiotic_revenue_categories()}}
                    )
                    and abs(datediff(day, invalid_transactions.transaction_datetime, transactions.transaction_datetime)) <= 7
                )
                or invalid_invoice.tag in {{anesthesia_hospitalization_revenue_categories()}}
                or (transactions.species = 'canine'
                    and invalid_transactions.tag = 60))
        )

        or (transactions.tag in {{alternative_therapy_revenue_categories()}}
            and ((abs(datediff(day, invalid_transactions.transaction_datetime, transactions.transaction_datetime)) <= 7
                    and (invalid_transactions.tag in {{dental_and_surgery_revenue_categories()}}
                        or invalid_transactions.tag = 60
                        or invalid_transactions.tag in {{invalid_pain_revenue_categories()}}
                        or invalid_transactions.tag in {{antibiotic_revenue_categories()}}
                        or invalid_transactions.tag in {{antihistamine_revenue_categories()}}
                        or invalid_transactions.tag in {{skin_topical_revenue_categories()}}
                        or invalid_transactions.tag in {{steroid_revenue_categories()}}
                        or invalid_transactions.tag in {{derm_steroid_revenue_categories()}})
                )
                or invalid_invoice.tag in {{anesthesia_hospitalization_revenue_categories()}})
        )
)
, pain_transactions as (
    select
        /* All transactions that were not invalidated by a transaction of a different type */
        transactions.server_odu_id
        , transactions.practice_odu_id
        , transactions.patient_odu_id
        , datediff(year, transactions.birth_date, transactions.transaction_datetime) as patient_age
        , transactions.species
        , transactions.transaction_odu_id
        , transactions.transaction_datetime
        , transactions.odu_updated_at_utc
        , transactions.invoice_odu_id
        , case
            when transactions.tag = 56 then 'Pain Diet'
            when transactions.tag = 251 then 'DMOAD'
            when transactions.tag in {{nsaid_revenue_categories()}} then 'NSAIDs'
            when transactions.tag = 285 then 'Supplements'
            when transactions.tag in {{mab_revenue_categories()}} then 'Monoclonal Antibody'
            when transactions.tag in {{analgesic_tag_revenue_categories()}} then 'Analgesic'
            when transactions.tag in {{gabapentin_tramadol_revenue_categories()}} then 'Gabapentin & Tramadol'
            when transactions.tag in {{anti_inflammatory_revenue_categories()}} then 'Anti-Inflammatory'
            when transactions.tag in {{opiate_revenue_categories()}} then 'Opiate'
            when transactions.tag in {{alternative_therapy_revenue_categories()}} then 'Alternative Therapies'
        end as rule
        , count(distinct case when transactions.tag in {{alternative_therapy_revenue_categories()}}
                            and (invalid_transactions.tag in (56, 78, 251, 274, 285)
                                or invalid_transactions.tag in {{anti_inflammatory_revenue_categories()}}
                                or invalid_transactions.tag in {{mab_revenue_categories()}}
                                or invalid_transactions.tag in {{gabapentin_tramadol_revenue_categories()}}
                            ) then invalid_transactions.transaction_odu_id end) as validating_purchases
    from int_pain_transactions as transactions
    left join invalid_transactions
        on transactions.server_odu_id = invalid_transactions.server_odu_id
        and transactions.patient_odu_id = invalid_transactions.patient_odu_id
        and datediff(months, invalid_transactions.transaction_datetime, transactions.transaction_datetime) between 0 and 12
    left join invalid_pain_transactions
        on transactions.server_odu_id = invalid_pain_transactions.server_odu_id
        and transactions.patient_odu_id = invalid_pain_transactions.patient_odu_id
        and transactions.transaction_odu_id = invalid_pain_transactions.transaction_odu_id
    where
        invalid_pain_transactions.patient_odu_id is null
        and (transactions.tag = 56 /* Joint Diet */
            or transactions.tag in {{analgesic_tag_revenue_categories()}}
            or transactions.tag in {{anti_inflammatory_revenue_categories()}}
            or transactions.tag = 251 /* DMOAD */
            or transactions.tag in {{nsaid_revenue_categories()}}
            or transactions.tag in {{opiate_revenue_categories()}}
            or transactions.tag = 285 /* Supplements */
            or transactions.tag in {{mab_revenue_categories()}}
            or transactions.tag in {{gabapentin_tramadol_revenue_categories()}}
            or transactions.tag in {{alternative_therapy_revenue_categories()}})
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    having
        rule != 'Alternative Therapies'
        or patient_age >= 3
        or validating_purchases >= 2
)
, incremental_pain_transactions as (
    select
        transactions.patient_odu_id
        , transactions.practice_odu_id
        , transactions.server_odu_id
        , transactions.species
        , transactions.transaction_odu_id
        , transactions.odu_updated_at_utc
        , transactions.invoice_odu_id
        , transactions.transaction_datetime
        , transactions.rule
        , max(pain_transactions.odu_updated_at_utc) as updated_at
        , count(distinct pain_transactions.transaction_datetime::date) as purchases
    from pain_transactions as transactions
    left join pain_transactions
        on transactions.server_odu_id = pain_transactions.server_odu_id
        and transactions.patient_odu_id = pain_transactions.patient_odu_id
        and transactions.rule = pain_transactions.rule
        and transactions.transaction_odu_id != pain_transactions.transaction_odu_id
        and transactions.transaction_datetime::date != pain_transactions.transaction_datetime::date
        and abs(datediff(month, pain_transactions.transaction_datetime, transactions.transaction_datetime)) < 12
        and transactions.rule in ('NSAIDs', 'Analgesic', 'Gabapentin & Tramadol', 'Supplements', 'Opiate', 'Alternative Therapies')
    where
        transactions.transaction_datetime >= add_months(date_trunc('month', current_date), -36)
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    having
        (
            transactions.rule not in ('NSAIDs', 'Anti-Inflammatory', 'Analgesic', 'Gabapentin & Tramadol', 'Supplements', 'Opiate', 'Alternative Therapies')
            or (transactions.rule in ('NSAIDs', 'Anti-Inflammatory', 'Analgesic', 'Gabapentin & Tramadol', 'Supplements', 'Opiate')
                and purchases > 1
            )
            or (transactions.rule in ('Alternative Therapies')
                and purchases > 2)
        )
)
select
    patient_odu_id
    , server_odu_id
    , practice_odu_id
    , species
    , transaction_odu_id
    , greatest(odu_updated_at_utc, updated_at) as odu_updated_at_utc
    , invoice_odu_id
    , transaction_datetime
    , rule
    , purchases
from incremental_pain_transactions
{% if is_incremental() %}
where greatest(odu_updated_at_utc, updated_at) >= (select dateadd('hour', -24, max(odu_updated_at_utc)) from {{ this }})
{% endif %}
