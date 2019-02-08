 
with source as (
  select * from source_data.listings
),
renamed as (
  select
    host_id,
    id as listing_id,
    review_scores_value,
    review_scores_location,
    review_scores_communication,
    review_scores_checkin,
    review_scores_cleanliness,
    review_scores_accuracy,
    review_scores_rating,
    number_of_reviews,
    availability_365,
    availability_90,
    availability_60,
    availability_30,
    maximum_nights,
    minimum_nights,
    accommodates,
    longitude,
    latitude,
    require_guest_phone_verification,
    require_guest_profile_picture,
    cancellation_policy,
    is_business_travel_ready,
    instant_bookable,
    jurisdiction_names,
    license,
    requires_license,
    has_availability,
    calendar_updated,
    extra_people,
    guests_included,
    cleaning_fee,
    security_deposit,
    monthly_price,
    weekly_price,
    nullif(replace(split_part(price, '$', 2), ',', ''), '') as price,
    square_feet,
    amenities,
    bed_type,
    beds,
    bedrooms,
    bathrooms,
    room_type,
    property_type,
    is_location_exact,
    country,
    country_code,
    smart_location,
    market,
    zipcode,
    state,
    city,
    neighbourhood_group_cleansed,
    neighbourhood_cleansed,
    neighbourhood,
    street,
    host_identity_verified,
    host_has_profile_pic,
    host_verifications,
    host_neighbourhood,
    is_superhost,
    acceptance_rate,
    response_rate,
    response_time,
    host_about,
    host_location,
    host_name,
    house_rules,
    interaction,
    access,
    transit,
    notes,
    neighborhood_overview,
    experiences_offered,
    description,
    space,
    summary,
    name,
    scrape,
    url,
    last_review,
    first_review,
    host_since
  from source
)

select * from renamed