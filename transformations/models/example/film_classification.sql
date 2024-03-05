SELECT
    film_id,
    title,
    user_rating,
    (CASE
        WHEN {{ 'user_rating' }} >= 4.5 THEN 'Excellent'
        WHEN {{ 'user_rating' }} >= 4.0 THEN 'Good'
        WHEN {{ 'user_rating' }} >= 3.0 THEN 'Average'
        ELSE 'Poor'
    END) AS rating_category,
    release_date
FROM {{ ref('films') }}