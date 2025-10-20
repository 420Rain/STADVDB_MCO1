import pandas as pd
from sqlalchemy import text
from scipy import stats
import math

class OLAP(object):
    def __init__(self, engine):
        """Class constructor for OLAP
        Arguments:
            engine {Engine} - An Engine instance for providing functionality
            for connections to a particular database.
        """
        self.engine = engine
    
    
    def query_1(self, minVotes: int = 5000, startYear: int = 2019, titleType: str = 'movie'):
        """Returns a table detailing the highest rated titles given the title 
        type in a given year.
        Arguments:
            minVotes {integer} -- Number for minimum amount of votes required for 
            a given title.
            startYear {integer} -- Year for when the title started airing/showing.
            titleType {string} -- Type of title (e.g. movie)
        Returns:
            int -- support for itemset in data
        """
        with self.engine.connect() as connection:
            
            query = text("""
                SELECT dt.primary_title, 
                ftr.average_rating,
                ftr.num_votes
                FROM dw_schema.fact_title_ratings AS ftr
                JOIN dw_schema.dim_title AS dt 
                ON ftr.title_key = dt.title_key
                JOIN dw_schema.dim_date AS dd 
                ON ftr.date_key = dd.date_key
                WHERE ftr.num_votes > :votes --@minVotes
                    AND dt.start_year = :year --@startYear
                    AND dt.title_type = :type --@titleType
                ORDER BY ftr.average_rating DESC;
            """)
            
            result = connection.execute(
                query, 
                {"votes": minVotes, "year": startYear, "type": titleType}
            )
            
        return result
    
    def query_2(self):
        with self.engine.connect() as connection:
            
            query = text("""
                WITH RollUpHierarchy AS (
                    SELECT
                        title_key,
                        title_type,
                        CASE
                            WHEN title_type IN ('tvEpisode', 'tvMiniSeries', 'tvMovie', 'tvPilot', 'tvSeries', 'tvShort', 'tvSpecial') THEN 'Television'
                            WHEN title_type IN ('movie', 'short', 'video') THEN 'Film'
                            ELSE 'Other'
                        END AS broad_type
                    FROM dw_schema.dim_title
                )
                SELECT broad_type, 
                    title_type, 
                    COUNT(*) AS number_of_titles
                FROM RollUpHierarchy
                GROUP BY ROLLUP (broad_type, title_type)
                ORDER BY broad_type, title_type;
            """)
            
            result = connection.execute(query)
            
        return result
    
    def query_3(self, minVotes: int = 5000):
        with self.engine.connect() as connection:
            
            query = text("""
                WITH RollUpHierarchy AS (
                    SELECT
                        title_key,
                        title_type,
                        CASE
                            WHEN title_type IN ('tvEpisode', 'tvMiniSeries', 'tvMovie', 'tvPilot', 'tvSeries', 'tvShort', 'tvSpecial') THEN 'Television'
                            WHEN title_type IN ('movie', 'short', 'video') THEN 'Film'
                            ELSE 'Other'
                        END AS broad_type
                    FROM dw_schema.dim_title
                )
                SELECT ruh.broad_type, 
                    COUNT(*) AS number_of_titles,
                    ROUND(AVG(ftr.average_rating), 2) AS overall_average_rating 
                FROM dw_schema.fact_title_ratings AS ftr
                JOIN RollUpHierarchy AS ruh 
                    ON ruh.title_key = ftr.title_key
                WHERE ruh.broad_type IN ('Television', 'Film')
                    AND ftr.num_votes > :votes --@minVotes
                GROUP BY ruh.broad_type
                ORDER BY overall_average_rating DESC;
            """)
            
            result = connection.execute(
                query,
                {"votes": minVotes}
            )
            
        return result
    
    def query_4_1(self, minVotes: int = 5000, minTitles: int = 5):
        with self.engine.connect() as connection:
            
            query = text("""
                SELECT dp.primary_name, 
                    COUNT(DISTINCT(dt.title_key)) AS number_of_titles,
                    ROUND(AVG(ftr.average_rating),2) AS average_ratings_of_titles
                FROM dw_schema.fact_title_principals AS ftp
                JOIN dw_schema.dim_person AS dp 
                    ON ftp.person_key = dp.person_key
                JOIN dw_schema.dim_title AS dt
                    ON ftp.title_key = dt.title_key
                JOIN dw_schema.fact_title_ratings AS ftr
                    ON ftp.title_key = ftr.title_key
                WHERE ftr.num_votes > :votes -- @minVotes
                GROUP BY dp.primary_name
                HAVING COUNT(DISTINCT(dt.title_key)) >= :titles -- @minTitles
                ORDER BY number_of_titles DESC,
                    average_ratings_of_titles DESC;
            """)
            
            result = connection.execute(
                query,
                {"votes": minVotes, "titles": minTitles}
            )
            
        return result
    
    def query_4_2(self, minVotes: int = 5000, role: str = 'director', minTitles: int = 5):
        with self.engine.connect() as connection:
            
            query = text("""
                SELECT dp.primary_name, 
                    COUNT(DISTINCT(dt.title_key)) AS number_of_titles,
                    ROUND(AVG(ftr.average_rating),2) AS average_ratings_of_titles
                FROM dw_schema.fact_title_principals AS ftp
                JOIN dw_schema.dim_person AS dp 
                    ON ftp.person_key = dp.person_key
                JOIN dw_schema.dim_title AS dt
                    ON ftp.title_key = dt.title_key
                JOIN dw_schema.fact_title_ratings AS ftr
                    ON ftp.title_key = ftr.title_key
                JOIN dw_schema.dim_role AS dr
                    ON ftp.role_key = dr.role_key
                WHERE ftr.num_votes > :votes -- @minVotes
                AND dr.category = :job -- @role/job 
                GROUP BY dp.primary_name
                HAVING COUNT(DISTINCT(dt.title_key)) >= :titles -- @minTitles
                ORDER BY number_of_titles DESC,
                    average_ratings_of_titles DESC;
            """)
            
            result = connection.execute(
                query,
                {"votes": minVotes, "job": role, "titles": minTitles}
            )
            
        return result
    
    def query_4_3(self, role: str = 'director', empName: str = 'Hayao Miyazaki'):
        with self.engine.connect() as connection:
            
            query = text("""
                SELECT dt.primary_title,
                    ftr.average_rating,
                    ftr.num_votes
                FROM dw_schema.fact_title_principals AS ftp
                JOIN dw_schema.dim_person AS dp 
                    ON ftp.person_key = dp.person_key
                JOIN dw_schema.dim_title AS dt
                    ON ftp.title_key = dt.title_key
                JOIN dw_schema.fact_title_ratings AS ftr
                    ON ftp.title_key = ftr.title_key
                JOIN dw_schema.dim_role AS dr
                    ON ftp.role_key = dr.role_key
                WHERE dr.category = :job -- @role/job
                    AND dp.primary_name = :name -- @empName
                ORDER BY ftr.average_rating DESC;
            """)
            
            result = connection.execute(
                query,
                {"job": role, "name": empName}
            )
            
        return result
    
    def query_5(self, minVotes: int = 5000, minRating: float = 6.0, maxRating: float = 10.0):
        with self.engine.connect() as connection:
            
            query = text("""
                SELECT dd.decade,
                    COUNT(ftr.title_key) number_of_films
                FROM dw_schema.fact_title_ratings AS ftr
                JOIN dw_schema.dim_title AS dt
                    ON ftr.title_key = dt.title_key
                JOIN dw_schema.dim_date AS dd
                    ON ftr.date_key = dd.date_key
                WHERE dt.title_type = 'movie'
                    AND ftr.num_votes > :votes -- @minVotes
                    AND ftr.average_rating > :min -- @minRating
                    AND ftr.average_rating < :max -- @maxRating
                GROUP BY dd.decade
                ORDER BY dd.decade;
            """)
            
            result = connection.execute(
                query,
                {"votes": minVotes, "min": minRating, "max": maxRating}
            )
            
        return result
    
    def query_6(self, seriesName: str = 'Steins;Gate'):
        with self.engine.connect() as connection:
            
            query = text("""
                SELECT 
                    ep.season_number,
                    COUNT(*) as number_of_episodes,
                    ROUND(AVG(ftr.average_rating),2) AS season_rating
                FROM dw_schema.fact_title_ratings AS ftr
                JOIN dw_schema.dim_title AS ep
                    ON ftr.title_key = ep.title_key
                JOIN dw_schema.dim_title AS sea
                    ON ep.parent_tconst = sea.tconstid
                WHERE 
                    sea.primary_title = :series -- @seriesName
                    AND ep.season_number IS NOT NULL
                GROUP BY ep.season_number
                ORDER BY ep.season_number;
            """)
            
            result = connection.execute(
                query,
                {"series": seriesName}
            )
            
        return result
    
    def query_7(self, minVotes: int = 5000):
        with self.engine.connect() as connection:
            
            query = text("""
                SELECT 
                    dt.genre_1 AS genre,
                    ftr.average_rating AS rating,
                    ftr.num_votes AS votes
                FROM dw_schema.fact_title_ratings AS ftr
                JOIN dw_schema.dim_title AS dt ON ftr.title_key = dt.title_key
                WHERE 
                    dt.title_type = 'movie'
                    AND ftr.num_votes > :votes -- @minVotes
                    AND dt.genre_1 IS NOT NULL
                ORDER BY dt.genre_1;
            """)
            
            result = connection.execute(
                query,
                {"votes": minVotes}
            )
            
        return result
    
    def t_test_1(self):
        with self.engine.connect() as connection:
            
            query = text("""
                WITH group_stats AS (
                    SELECT
                        t.is_adult,
                        COUNT(r.average_rating) AS n,
                        AVG(r.average_rating) AS mean,
                        VAR_SAMP(r.average_rating) AS variance
                    FROM dw_schema.fact_title_ratings AS r
                    JOIN dw_schema.dim_title AS t
                        ON r.title_key = t.title_key
                    WHERE t.title_type = 'movie'
                    GROUP BY t.is_adult
                ), 
                adult_stats AS (
                    SELECT
                         n,
                         mean,
                         variance
                    FROM group_stats
                    WHERE is_adult = TRUE
                ), 
                non_adult_stats AS (
                    SELECT
                         n,
                         mean,
                         variance
                    FROM group_stats
                    WHERE is_adult = FALSE
                )
                SELECT
                    (non_adult_stats.mean - adult_stats.mean) / SQRT((non_adult_stats.variance / non_adult_stats.n) + (adult_stats.variance / adult_stats.n)) AS t_statistic_adult_vs_non_adult_rating,
                    non_adult_stats.n AS n_non_adult,
                    adult_stats.n AS n_adult
                FROM adult_stats, non_adult_stats;
            """)
            
            result = connection.execute(query)
            
        return result
    
    def t_test_2(self):
        with self.engine.connect() as connection:
            
            query = text("""
                WITH group_stats AS (
                    SELECT
                            d.century,
                            COUNT(r.average_rating) AS n,
                            AVG(r.average_rating) AS mean,
                            VAR_SAMP(r.average_rating) AS variance
                    FROM dw_schema.fact_title_ratings AS r
                    JOIN dw_schema.dim_title AS t
                        ON r.title_key = t.title_key
                    JOIN dw_schema.dim_date AS d
                        ON t.start_year = d.year
                    WHERE t.title_type = 'movie' AND d.century IN (1800, 1900)
                    GROUP BY d.century
                ), century_19_stats AS (
                    SELECT
                            n,
                            mean,
                            variance
                    FROM group_stats
                    WHERE century = 1800
                ), century_20_stats AS (
                    SELECT
                            n,
                            mean,
                            variance
                    FROM group_stats
                    WHERE century = 1900
                )
                SELECT
                    (century_19_stats.mean - century_20_stats.mean) / SQRT((century_19_stats.variance / century_19_stats.n) + (century_20_stats.variance / century_20_stats.n)) AS t_statistic_century_rating_comparison,
                    century_19_stats.n AS n_19th,
                    century_20_stats.n AS n_20th
                FROM century_19_stats, century_20_stats;
            """)
            
            result = connection.execute(query)
            
        return result
    
    def t_test_3(self):
        with self.engine.connect() as connection:
            
            query = text("""
                WITH group_stats AS (
                    SELECT
                            t.genre_1,
                            COUNT(r.num_votes) AS n,
                            AVG(r.num_votes) AS mean,
                            VAR_SAMP(r.num_votes) AS variance
                    FROM dw_schema.fact_title_ratings AS r
                    JOIN dw_schema.dim_title AS t
                        ON r.title_key = t.title_key
                    WHERE t.genre_1 IN ('Action', 'Comedy') AND t.title_type = 'movie'
                    GROUP BY t.genre_1
                ), action_stats AS (
                    SELECT
                            n,
                            mean,
                            variance
                    FROM group_stats
                    WHERE genre_1 = 'Action'
                ), comedy_stats AS (
                    SELECT
                            n,
                            mean,
                            variance
                    FROM group_stats
                    WHERE genre_1 = 'Comedy'
                )
                SELECT
                    (action_stats.mean - comedy_stats.mean) / SQRT((action_stats.variance / action_stats.n) + (comedy_stats.variance / comedy_stats.n)) AS t_statistic_action_vs_comedy_votes, 
                    action_stats.n AS n_action, 
                    comedy_stats.n AS n_comedy
                FROM action_stats, comedy_stats;
            """)
            
            result = connection.execute(query)
            
        return result
    
    def t_test_4(self):
        with self.engine.connect() as connection:
            
            query = text("""
                WITH group_stats AS (
                    SELECT
                            d.decade,
                            COUNT(t.end_year - t.start_year) AS n,
                            AVG(t.end_year - t.start_year) AS mean,
                            VAR_SAMP(t.end_year - t.start_year) AS variance
                    FROM dw_schema.dim_title AS t
                    JOIN dw_schema.dim_date AS d
                        ON t.start_year = d.year
                    WHERE t.title_type = 'tvSeries'
                        AND t.end_year IS NOT NULL
                        AND t.start_year IS NOT NULL
                        AND t.end_year >= t.start_year
                        AND d.decade IN (1990, 2010)
                    GROUP BY d.decade
                ), stats_1990s AS (
                    SELECT
                            n,
                            mean,
                            variance
                    FROM group_stats
                    WHERE decade = 1990
                ), stats_2010s AS (
                    SELECT
                            n,
                            mean,
                            variance
                    FROM group_stats
                    WHERE decade = 2010
                )
                SELECT
                    (stats_1990s.mean - stats_2010s.mean) / SQRT((stats_1990s.variance / stats_1990s.n) + (stats_2010s.variance / stats_2010s.n)) AS t_statistic_tv_series_lifespan,
                    stats_1990s.n AS n_1990s,
                    stats_2010s.n AS n_2010s
                FROM stats_1990s, stats_2010s;
            """)
            
            result = connection.execute(query)
            
        return result
    
    def t_test_5(self):
        with self.engine.connect() as connection:
            
            query = text("""
                WITH group_stats AS (
                    SELECT
                        CASE
                            WHEN t.parent_tconst IS NOT NULL
                            THEN 'Franchise'
                            ELSE 'Standalone'
                        END AS film_type,
                        COUNT(r.num_votes) AS n,
                        AVG(r.num_votes) AS mean,
                        VAR_SAMP(r.num_votes) AS variance
                    FROM dw_schema.fact_title_ratings as r
                    JOIN dw_schema.dim_title as t
                    ON r.title_key = t.title_key
                    GROUP BY film_type
                ), franchise_stats AS (
                    SELECT
                        n,
                        mean,
                        variance
                    FROM group_stats
                    WHERE film_type = 'Franchise'
                ), standalone_stats AS (
                    SELECT
                        n,
                        mean,
                        variance
                    FROM group_stats
                    WHERE film_type = 'Standalone'
                )
                SELECT
                    (franchise_stats.mean - standalone_stats.mean) / SQRT((franchise_stats.variance / franchise_stats.n) + (standalone_stats.variance / standalone_stats.n)) AS t_statistic_franchise_vs_standalone_votes,
                    franchise_stats.n AS n_franchise,
                    standalone_stats.n AS n_standalone
                FROM franchise_stats, standalone_stats;
            """)
            
            result = connection.execute(query)
            
        return result
    
    def print_p_value_report(self, t_stat, n1, n2, s1, s2, alpha=0.05, tail='two-tailed'):
        df = min(n1 - 1, n2 - 1)

        if tail == 'two-tailed':
            p_value = 2 * (1 - stats.t.cdf(abs(t_stat), df))
        elif tail == 'left':
            p_value = stats.t.cdf(t_stat, df)
        elif tail == 'right':
            p_value = 1 - stats.t.cdf(t_stat, df)
        else:
            raise ValueError("tail must be 'two-tailed', 'left', or 'right'")

        print(f"---T-test Statistical Analysis ---")
        print(f"Input T-statistic: {t_stat}")
        print(f"Sample Size ({s1}): {n1}")
        print(f"Sample Size ({s2}): {n2}")
        print("-----------------------------------")
        print(f"Calculated P-value: {p_value:.6f}")

        if p_value < alpha:
            print("\nConclusion: The result is statistically significant (There is a significant difference between the two samples).")
        else:
            print("\nConclusion: The result is not statistically significant (There is NO significant difference between the two samples).")
    