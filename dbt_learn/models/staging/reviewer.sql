{% set neighbourhoods = dbt_learn.get_column_values_alphabetically(
	table="dbt_lev.listings", 
	column='neighbourhood', 
	max_records=100)
%}


select 
	reviewer_name,
 	count(distinct(reviews_id)),
 	min(review_date), 
	{% for neighbourhood  in neighbourhoods %}
		{% set neighbourhood_clean = neighbourhood | lower | 
			replace(" ", "_") | replace("/", "_") | replace("-", "")%}

	sum(case when neighbourhood = '{{neighbourhood}}' then 1 else 0 end) 
		as {{neighbourhood_clean}}_count

	{{- "," if not loop.last -}}

	{%- endfor %}

	from dbt_lev.reviews 
		join dbt_lev.listings 
			using (listing_id)
		group by 1

