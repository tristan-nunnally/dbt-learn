

select 
	zipcode, 
	{% for property_type in ['house', 'apartment', 'townhouse']%}
	sum(case when lower(property_type) = '{{property_type}}' then 1 else 0 end) 
	as {{property_type}}_count

	{{- "," if not loop.last -}}

	{%- endfor %}

	from source_data.listings

	group by 1

	limit 100