Get all active elite users reviews 

dataframe eliteReviews = {
	SELECT * FROM review_table r
		JOIN user_table u
		ON
		(
			(r.user_id = u.user_id)
			AND
			(YEAR(r.date) IN (U.ELITE))
		)
	}

foreach eliteReview:
	dateElite, idResturant, idUser, EliteRating = select data, resturant_id, user_id, rating

	dataframe beforeReviews = {
	SELECT * FROM review_table r
		WHERE
			r.date > dateElite
				AND
			r.resutrant_id = idResturant
		ORDER BY date ASC
		LIMIT 10
	}

	ratingBefore = 0;
	ratingBeforeCount = 0;
	foreach beforeReivew:
		rating = select rating;
		ratingBefore += rating;
		ratingBeforeCount++;

	averageRatingBefore = ratingBefore/ratingBeforeCount;


	dataframe afterReviews = {
	SELECT * FROM review_table r
		WHERE
			r.date < dateElite
				AND
			r.resutrant_id = idResturant
		ORDER BY date DESC
		LIMIT 10
	}

	ratingAfter = 0;
	ratingAfterCount = 0;
	foreach afterReivew:
		rating = select rating;
		ratingAfter += rating;
		ratingBAfterCount++;

	averageRatingAfter = ratingAfter/ratingAfterCount;

	//assess influence


	//relatively equal
	withinValue = .1;


	if(averageRatingBefore + withinValue < EliteRating && averageRatingBefore + withinValue < averageRatingAfter)
		value = 1

	else if (averageRatingBefore > EliteRating + withinValue && averageRatingBefore > averageRatingAfter + withinValue)
		value = 1

	else if(EliteRate + withinValue <= averageRatingAfter && EliteRate + withinValue >= averageRatingAfter)
		value = 1
	
	else
		value = 0



	key = idUser
	
	Tuple<String, Tuple<Int, Int>> = new Tuple(key, new Tuple(value, reviewCount = 1));

	reducer{
		value += value
		reviewCount += reviewCount

	}

	write to file{
		print key(User_id) + " " + (value/(double)reviewCount);
	}

//Note: date is a string. May need to change to a Date object/type to do comparisons on it.


Now assume that we have a a file containing
eliteUserId influenceValue

influencevalue >= .7 mean that this elite user is influencal 

