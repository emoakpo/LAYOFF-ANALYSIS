SELECT 
    *
FROM
    layoffs;

-- DATA CLEANING/WRANGLING
-- 1. REMOVE DUPLICATE
-- 2. STANDARDIZE DATE
-- 3. NULL VALUE AND BLANK VALUE
-- 4. REMOVE ANY COLUMN NOT NEEDED

-- STEP 1
-- Create another table to work with because you dont want to temper the original table
-- I Created a table called layoffs_working exactly like the layoffs table
CREATE TABLE layoffs_working
LIKE layoffs;
-- Now insert the content from the layoffs table
INSERT layoffs_working
SELECT * FROM layoffs;

-- STEP 2 REMOVE DUPLICATE
-- Create a ROW NUMBER to match all the columns

SELECT *,
ROW_NUMBER () OVER (
partition by company,industry,total_laid_off,
percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
FROM layoffs_working;

-- CREATE A CTE FOR THE ABOVE OR A SUB-QUERY, TO FILTER WHERE THE ROW NUMBR IS > 1
WITH duplicate_cte AS (SELECT *,
ROW_NUMBER () OVER (
partition by company,industry,total_laid_off,
percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
FROM layoffs_working)
SELECT * FROM duplicate_cte
WHERE row_num > 1;

-- We dont want remove all the Duplicate but Only one

/*
WITH duplicate_cte AS (SELECT *,
ROW_NUMBER () OVER (
partition by company,industry,total_laid_off,
percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
FROM layoffs_working)
DELETE 
FROM duplicate_cte
WHERE row_num > 1; */ -- this cannot work beacuse its a temp table

-- copy the above and right click on copy to clipboard, Create statement and paste.
-- CREATE ANOTHER TABLE
CREATE TABLE `layoffs_working2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; 

INSERT INTO layoffs_working2
SELECT *,
ROW_NUMBER () OVER (
partition by company,industry,total_laid_off,
percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
FROM layoffs_working;
-- DELETING THE DUPLICATING
DELETE FROM layoffs_working2
WHERE row_num > 1;
-- DROPPING THE row_num column
ALTER TABLE layoffs_working2
DROP COLUMN row_num;
-- STANDARDISING DATA.. This is finding issue in your data and fixing it
-- TRIM to remove white spaces
SELECT company, TRIM(company) 
FROM layoffs_working2;
-- Update table
UPDATE layoffs_working2
SET company = TRIM(company);

-- Checking at the Industry column
SELECT DISTINCT industry
FROM layoffs_working2
ORDER BY 1;

SELECT *
FROM layoffs_working2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_working2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';
-- NO ERROR ON LOCATION
SELECT DISTINCT location
FROM layoffs_working2
ORDER BY 1;


SELECT DISTINCT country
FROM layoffs_working2
ORDER BY 1;
-- correct using trailing (to remove from the end that is not a white space
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_working2
ORDER BY 1;
-- update tabel
UPDATE layoffs_working2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- changing date format especially wen dealing with a time series analysis
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_working2;

UPDATE layoffs_working2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date` FROM layoffs_working2;

-- CONVERT  the `date` to a date datatype
ALTER TABLE layoffs_working2
MODIFY COLUMN `date` DATE;

-- WORKING WITH NULL AND BLANK VALUES
SELECT * FROM layoffs_working2
WHERE total_laid_off is NULL 
and percentage_laid_off IS NULL;

SELECT * FROM layoffs_working2
WHERE industry is NULL OR industry ='';

-- CHECKING FOR AIRBNB
-- correcting the industry based on another of same company
SELECT * FROM layoffs_working2
WHERE company = 'AirBNB';

-- set blank to null first
UPDATE layoffs_working2
SET industry = NULL
WHERE industry = '';


SELECT * 
FROM layoffs_working2 s1
JOIN layoffs_working2 s2
	ON s1.company =  s2.company 
    AND s1.location = s1.location
WHERE (s1.industry IS NULL OR s1.industry = '')
AND s2.industry IS NOT NULL;

UPDATE layoffs_working2 s1
JOIN layoffs_working2 s2
	ON s1.company =  s2.company 
SET s1.industry = s2.industry
WHERE s1.industry IS NULL
AND s2.industry IS NOT NULL;


-- deleting those with No layoff
SELECT * FROM layoffs_working2
WHERE total_laid_off is NULL 
and percentage_laid_off IS NULL;

DELETE FROM layoffs_working2
WHERE total_laid_off is NULL 
and percentage_laid_off IS NULL;

-- VIEWING ALL
select * from layoffs_working2;
--------------------------  END ------------------------------------


-- EXPLORATORY DATA ANALYSIS (EDA)---------------------------
-- the Eda has no definite agenda, its about exploring the dataset and looking and providing insight at everyday

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_working2;

SELECT * 
FROM layoffs_working2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions desc;

SELECT company, SUM(total_laid_off) AS 'TOTAL LAID OFF'
FROM layoffs_working2
GROUP BY 1
ORDER BY 2 DESC;


SELECT min(`date`), MAX(`date`)
FROM layoffs_working2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_working2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_working2
GROUP BY 1
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_working2
GROUP BY 1
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_working2
GROUP BY 1
ORDER BY 2 DESC;

-- TOTAL LAID OFF BY MONTHS
SELECT SUBSTRING(`date`,1,7) as `MONTH`, SUM(total_laid_off)
FROM layoffs_working2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- ROLLING SUM/CUMMULATIVE 
WITH Rolling_sum_Total AS (
SELECT SUBSTRING(`date`,1,7) as `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_working2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC)
SELECT `MONTH`, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_sum_Total;


-- ROLLING SUM/CUMMULATIVE TOTAL BY MONTH AND TOTAL LAID OFF
WITH Rolling_sum_Total AS (
SELECT SUBSTRING(`date`,1,7) as `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_working2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC)
SELECT `MONTH`, total_off, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_sum_Total;

-- COMPANY AGAINST TOTAL LAID OFF AND YEAR
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_working2
GROUP BY company, YEAR(`date`)
ORDER BY 3 desc;

-- who laid off the most people per year using rank
WITH Company_Year (company, YEARS, TOTAL_LAID_OFF) AS (
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_working2
GROUP BY company, YEAR(`date`) )
SELECT *, DENSE_RANK() OVER (PARTITION BY YEARS ORDER BY total_laid_off desc) AS RANKING
FROM Company_Year
WHERE YEARS IS NOT NULL
ORDER BY Ranking ASC;


WITH Company_Year (company, YEARS, TOTAL_LAID_OFF) AS (
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_working2
GROUP BY company, YEAR(`date`)),
Company_Year_Rank AS (
SELECT *, DENSE_RANK() OVER (PARTITION BY YEARS ORDER BY total_laid_off desc) AS RANKING
FROM Company_Year
WHERE YEARS IS NOT NULL
)
SELECT * FROM Company_Year_Rank
WHERE Ranking <= 5;

-- END -----------------------------------------------


/* QUESTIONS
1. What Industries have been most affected by layoffs */

SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM layoffs_working2
GROUP BY industry
ORDER BY total_laid_off DESC;

/* QUESTIONS
2. Which countries have experienced the highest percentage of layoffs */

SELECT country, AVG(percentage_laid_off) AS avg_laid_off
FROM layoffs_working2
GROUP BY country
ORDER BY avg_laid_off DESC;

/* QUESTIONS
3. Has the rate of layoffs increased or decreased over time */

SELECT YEAR(date) AS year, SUM(total_laid_off) AS total_laid_off
FROM layoffs_working2
WHERE YEAR(date) IS NOT NULL
GROUP BY year
ORDER BY year ASC;

/* QUESTIONS
4. Are there any correlations between company size (based on funding raised) and the likelihood of layoffs? */

SELECT AVG(percentage_laid_off) AS avg_laid_off, 
CASE 
WHEN funds_raised_millions < 20000 THEN 'Small Company'
WHEN funds_raised_millions BETWEEN 20000 and 60000 THEN 'Medium Company'
ELSE 'Large Company'
END AS Company_size
FROM layoffs_working2
GROUP BY 2
ORDER BY 2 ASC;


/* QUESTIONS
5. Which companies have laid off the most employees? */

SELECT company, SUM(total_laid_off) AS total_laid_off
FROM layoffs_working2
GROUP BY company
ORDER BY total_laid_off DESC;

/* QUESTIONS
6. How have layoffs affected companies in different stages of growth? */

SELECT stage, AVG(percentage_laid_off) AS avg_laid_off
FROM layoffs_working2
GROUP BY stage
ORDER BY avg_laid_off DESC;


