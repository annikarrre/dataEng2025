with source_data as (
    select * 
    from bronze.medications_monthly
),
cleaned_source as (
    select 
        cast(year as Int32) as year,
        cast(month as Int32) as month,
        trim(upper(ingredient)) as ingredient_name,
        trim(package) as package_name,
        cast(packages_count as Float64) as packages_count_num,
        cast(total_amount as Decimal(18,2)) as total_amount_num,
        cast(hk_amount as Decimal(18,2)) as hk_amount_num,
        cast(copay_with_trh as Decimal(18,2)) as copay_with_trh_num,
        cast(copay_no_trh as Decimal(18,2)) as copay_no_trh_num,
        cast(over_ref_price as Decimal(18,2)) as over_ref_price_num,
        cast(prescriptions as Float64) as prescriptions_num,
        cast(persons as Float64) as persons_num
    from source_data
    where year is not null
      and cast(year as Int32) between 2000 and toYear(today())
      and total_amount is not null
      and prescriptions is not null
      and persons is not null
      and packages_count is not null
),
cleaned_and_aggregated as (
    select 
        year,
        month,
        ingredient_name,
        package_name,
        -- aggregations only
        cast(round(sumIf(packages_count_num, packages_count_num IS NOT NULL), 0) as Int32) as total_packages,
        cast(round(sumIf(total_amount_num, total_amount_num IS NOT NULL), 2) as Decimal(18,2)) as total_amount,
        cast(round(sumIf(hk_amount_num, hk_amount_num IS NOT NULL), 2) as Decimal(18,2)) as health_insurance_amount,
        cast(round(sumIf(copay_with_trh_num, copay_with_trh_num IS NOT NULL), 2) as Decimal(18,2)) as copay_with_threshold,
        cast(round(sumIf(copay_no_trh_num, copay_no_trh_num IS NOT NULL), 2) as Decimal(18,2)) as copay_without_threshold,
        cast(round(sumIf(over_ref_price_num, over_ref_price_num IS NOT NULL), 2) as Decimal(18,2)) as amount_over_reference_price,
        cast(round(sumIf(prescriptions_num, prescriptions_num IS NOT NULL), 0) as Int32) as total_prescriptions,
        cast(round(sumIf(persons_num, persons_num IS NOT NULL), 0) as Int32) as total_persons
    from cleaned_source
    group by 
        year, 
        month, 
        ingredient_name, 
        package_name
)
select *
from cleaned_and_aggregated