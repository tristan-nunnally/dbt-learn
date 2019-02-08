with source as (

  select * from source_data.listing_history

),

renamed as (

    select

       	listing_id,
		nullif(replace(split_part(price, '$', 2), ',', ''), '') as price,
		available,
		date

    from source

)

select * from renamed
