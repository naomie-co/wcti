SELECT 
	rental.customer_id, customer.last_name AS "NOM", customer.first_name AS "PRENOM", COUNT(*) AS total_rent, address.address, address.latitude, address.longitude
FROM
	rental
JOIN customer
ON rental.customer_id = customer.customer_id
JOIN address
ON customer.address_id = address.address_id
GROUP BY 
	customer_id
ORDER BY
	total_rent DESC LIMIT 1;