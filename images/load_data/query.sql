select p.given_name, p.family_name, 
DATE_FORMAT(p.date_of_birth,"%%Y-%%m-%%d") date_of_birth, 
c.city_name birth_city, 
r.region_name birth_region, co.country_name birth_country 
FROM people p
join cities c on c.city_id = p.Birth_city_id
join regions r on r.region_id = c.region_id
join countries co on co.country_id = r.country_id
order by 1