with source as (

  select * from source_data.reviews

),

renamed as (

    select

        id as reviews_id,
        review,
        listing_id,
        comments,
        reviewer_name,
        date as review_date

    from source

)

select * from renamed
