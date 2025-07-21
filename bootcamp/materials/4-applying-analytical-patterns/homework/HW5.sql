


--Task 1 -----
 -- create a state change tracker table
CREATE TABLE player_state_tracker (
	player_name TEXT,
	first_active_year INTEGER,
	last_active_year INTEGER,
	active_state TEXT,
	years_active INTEGER [],
	current_season INTEGER,
	PRIMARY KEY(player_name, current_season)
);

-- incremental query to insert into player_state_tracker



INSERT INTO player_state_tracker
WITH last_season AS (
    SELECT * FROM player_state_tracker
    WHERE current_season = 2002

), 
this_season AS (
     SELECT * FROM player_seasons
    WHERE season = 2003
)
SELECT
        COALESCE(ls.player_name, ts.player_name) as player_name,
        COALESCE(ls.first_active_year, ts.season) as first_active_year,
        COALESCE(ts.season, ls.last_active_year) as last_active_year,
     	CASE
		 WHEN ls.first_active_year IS NULL AND ts.player_name IS NOT NULL THEN 'New'
		 WHEN ls.last_active_year < ls.current_season AND ts.player_name IS NULL THEN 'Stayed Retired'
		 WHEN ls.last_active_year < ls.current_season AND ts.player_name IS NOT NULL THEN 'Returned from Retirement'
		 WHEN ls.last_active_year = ls.current_season AND ts.player_name IS NULL THEN 'Retired'	 
		 ELSE 'Continued Playing'
		END as active_state,
        COALESCE(ls.years_active,
            ARRAY[]::INTEGER[]
            ) || CASE 
					WHEN ts.season IS NOT NULL THEN ARRAY[ts.season]
                ELSE ARRAY[]::INTEGER[] END
            as years_active,
         2003 AS current_season
    FROM last_season ls
    FULL OUTER JOIN this_season ts
    ON ls.player_name = ts.player_name;



	-- select * from player_state_tracker where player_name='Michael Jordan' -- Successfully verified the cases


-- TASK 2 -- 

-- Create table to join and aggregate based on player and team, player and season and team 
drop table joined_player_team_season

CREATE TABLE joined_player_team_season AS (
	SELECT gd.player_name AS player_name, 
		gd.team_id AS team_id, 
		EXTRACT(YEAR FROM g.game_date_est)::INT AS season,
		SUM(CASE 
			WHEN gd.team_id = g.home_team_id THEN g.home_team_wins
			ELSE 1 - g.home_team_wins
		END) AS total_won,
		SUM(COALESCE(gd.pts,0)) AS total_pts
		FROM game_details gd LEFT JOIN games g
		ON gd.game_id = g.game_id
		GROUP BY GROUPING SETS (
			(gd.player_name, gd.team_id),
			(gd.player_name, EXTRACT(YEAR FROM g.game_date_est)::INT),
			(gd.team_id)
		)
);


--- Who scored the most points for one team? 
SELECT player_name, team_id, total_pts
FROM joined_player_team_season
WHERE player_name IS NOT NULL AND team_id IS NOT NULL AND season IS NULL
ORDER BY total_pts DESC
LIMIT 1;

-- "Giannis Antetokounmpo" scored 15591 for team id 1610612749


-- Who scored the most points in one season?
SELECT player_name, season, total_pts
FROM joined_player_team_season
WHERE player_name IS NOT NULL AND season IS NOT NULL AND team_id IS NULL
ORDER BY total_pts DESC
LIMIT 1;
-- James Harden in 2019 scored 3506 pts


-- Which team has won the most games? 
WITH most_won_team_id AS (
	SELECT team_id
	FROM joined_player_team_season
	WHERE team_id IS NOT NULL AND player_name IS NULL AND season IS NULL
	ORDER BY total_won DESC
	LIMIT 1;
)  

-- TO FIND THE TEAM ABBREVIATION

SELECT DISTINCT(team_abbreviation) from game_details WHERE team_id = (
	SELECT team_id
	FROM joined_player_team_season
	WHERE team_id IS NOT NULL AND player_name IS NULL AND season IS NULL
	ORDER BY total_won DESC
	LIMIT 1
)

-- SO GSW OR GOLDEN STATE WARRIORS HAVE WON THE MOST GAMES


-- TASK 3 ---

-- What is the most games a team has won in a 90 game stretch?
WITH team_results AS (
    SELECT
        gd.game_id,
        gd.team_id,
        gd.team_abbreviation,
        g.game_date_est,
        CASE
            WHEN gd.team_id = g.home_team_id AND g.home_team_wins = 1 THEN 1
            WHEN gd.team_id = g.visitor_team_id AND g.home_team_wins = 0 THEN 1
            ELSE 0
        END AS game_wins
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
),
team_games AS (
    SELECT
        game_id,
        team_id,
        team_abbreviation,
        game_date_est,
        game_wins,
        ROW_NUMBER() OVER
            (PARTITION BY team_id
            ORDER BY game_date_est) AS game_number
    FROM team_results
    GROUP BY game_id, team_id, team_abbreviation, game_date_est, game_wins
),
rolling_wins AS (
    SELECT
        t1.team_id,
        t1.team_abbreviation,
        t1.game_number,
        SUM(t2.game_wins) AS wins_in_90_games
    FROM
        team_games t1
    JOIN
        team_games t2
    ON
        t1.team_id = t2.team_id
        AND t2.game_number BETWEEN t1.game_number AND t1.game_number + 89
    GROUP BY
        t1.team_id, t1.team_abbreviation, t1.game_number
)
SELECT
    team_id,
    team_abbreviation,
    MAX(wins_in_90_games) AS max_wins_in_90_games
FROM
    rolling_wins
GROUP BY
    team_id, team_abbreviation
ORDER BY
    max_wins_in_90_games DESC
LIMIT 1;

-- How many games in a row did LeBron James score over 10 points a game?
WITH starter AS (
SELECT g.game_date_est,
       player_name,
       team_id,
       team_abbreviation,
       pts,
           CASE
              WHEN pts > 10 THEN 1
              ELSE 0
           END AS scored_over_10
    FROM game_details gd
             JOIN games g on gd.game_id = g.game_id
    WHERE player_name = 'LeBron James'
),
lagged AS (
SELECT
    *,
    LAG(scored_over_10) OVER (PARTITION BY player_name ORDER BY game_date_est) AS scored_over_10_before
FROM starter
),
streak_change AS (
SELECT
    *,
    CASE WHEN scored_over_10 <> scored_over_10_before THEN 1 ELSE 0 END as streak_changed
FROM lagged
),
streak_identified AS (
    SELECT *,
           SUM(streak_changed) OVER (PARTITION BY player_name ORDER BY game_date_est) AS streak_identifier
    FROM streak_change
),
record_counts AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY player_name, streak_identifier ORDER BY game_date_est) AS streak_length
    FROM streak_identified
    WHERE scored_over_10 = 1 -- учитывать только игры с более чем 10 очками
    ),
ranked AS (
SELECT *,
       MAX(streak_length) OVER (PARTITION BY player_name, streak_identifier) AS max_streak_length
FROM record_counts
)
SELECT DISTINCT
    player_name,
    max_streak_length AS longest_streak
FROM ranked
ORDER BY longest_streak DESC
LIMIT 1;






	





