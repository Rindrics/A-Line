with profiles as (
    select * from {{ source('ocean_observations', 'fact_profiles') }}
),
observations as (
    select * from {{ source('ocean_observations', 'dim_observations') }}
),
depth_zones as (
    select
	pressure,
	case
	    when pressure <= 50 then 1  -- surface
	    when pressure <= 200 then 2  -- subsurface
	    when pressure <= 1000 then 3 -- mid-layer
	    else 4                       -- deep-layer
	end as depth_zone_id,
	case
	    when pressure <= 50 then 'surface'
	    when pressure <= 200 then 'subsurface'
	    when pressure <= 1000 then 'mid-layer'
	    else 'deep-layer'
	end as zone_name
    from (select distinct pressure from profiles)
)
select
    o.observation_id,
    o.station_id,
    dz.depth_zone_id,
    avg(p.temperature) as avg_temp,
    avg(p.salinity) as avg_salinity,
    case
	when avg(p.temperature) > 15 and avg(p.salinity) > 34.5 then 'kuroshio'
	when avg(p.temperature) < 5 and avg(p.salinity) < 33.5 then 'oyashio'
	else 'mixed'
    end as water_mass_type,
    count(*) as n_observations
from profiles p
join observations o using (observation_id)
join depth_zones dz using (pressure)
group by observation_id, station_id, depth_zone_id
