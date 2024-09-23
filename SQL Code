SELECT 
    *
FROM
    layoffs;

-- DATA CLEANING/WRANGLING
-- 1. REMOVE DUPLICATE
-- 2. STANDARDIZE DATE
-- 3. NULL VALUE AND BLANK VALUE
-- 4. REMOVE ANY COLUMN NOT NEEDED

CREATE TABLE layoffs_working LIKE layoffs;
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

CREATE TABLE `layoffs_working2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL,
    `row_num` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI; 

INSERT INTO layoffs_working2
SELECT *,
ROW_NUMBER () OVER (
partition by company,industry,total_laid_off,
percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
FROM layoffs_working;
-- DELETING THE DUPLICATING
DELETE FROM layoffs_working2 
WHERE
    row_num > 1;

-- DROPPING THE row_num column

ALTER TABLE layoffs_working2
DROP COLUMN row_num;

-- STANDARDISING DATA.. This is finding issue in your data and fixing it
-- TRIM to remove white spaces

SELECT 
    company, TRIM(company)
FROM
    layoffs_working2;

-- Update table
UPDATE layoffs_working2 
SET 
    company = TRIM(company);

-- Checking at the Industry column
SELECT DISTINCT
    industry
FROM
    layoffs_working2
ORDER BY 1;


SELECT 
    *
FROM
    layoffs_working2
WHERE
    industry LIKE 'Crypto%';

UPDATE layoffs_working2 
SET 
    industry = 'Crypto'
WHERE
    industry LIKE 'Crypto%';

-- NO ERROR ON LOCATION

SELECT DISTINCT
    location
FROM
    layoffs_working2
ORDER BY 1;


SELECT DISTINCT
    country
FROM
    layoffs_working2
ORDER BY 1;

-- correct using trailing (to remove from the end that is not a white space
SELECT DISTINCT
    country, TRIM(TRAILING '.' FROM country)
FROM
    layoffs_working2
ORDER BY 1;
-- update tabel
UPDATE layoffs_working2 
SET 
    country = TRIM(TRAILING '.' FROM country)
WHERE
    country LIKE 'United States%';

-- changing date format especially wen dealing with a time series analysis
SELECT 
    `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM
    layoffs_working2;

UPDATE layoffs_working2 
SET 
    `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT 
    `date`
FROM
    layoffs_working2;

-- CONVERT  the `date` to a date datatype
ALTER TABLE layoffs_working2
MODIFY COLUMN `date` DATE;

-- WORKING WITH NULL AND BLANK VALUES
SELECT 
    *
FROM
    layoffs_working2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;


SELECT 
    *
FROM
    layoffs_working2
WHERE
    industry IS NULL OR industry = '';

-- CHECKING FOR AIRBNB
-- correcting the industry based on another of same company

SELECT 
    *
FROM
    layoffs_working2
WHERE
    company = 'AirBNB';


-- set blank to null first
UPDATE layoffs_working2 
SET 
    industry = NULL
WHERE
    industry = '';


SELECT 
    *
FROM
    layoffs_working2 s1
        JOIN
    layoffs_working2 s2 ON s1.company = s2.company
        AND s1.location = s1.location
WHERE
    (s1.industry IS NULL OR s1.industry = '')
        AND s2.industry IS NOT NULL;

UPDATE layoffs_working2 s1
        JOIN
    layoffs_working2 s2 ON s1.company = s2.company 
SET 
    s1.industry = s2.industry
WHERE
    s1.industry IS NULL
        AND s2.industry IS NOT NULL;


-- deleting those with No layoff
SELECT 
    *
FROM
    layoffs_working2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

DELETE FROM layoffs_working2 
WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;

-- VIEWING ALL
SELECT 
    *
FROM
    layoffs_working2;
--------------------------  END ------------------------------------


-- EXPLORATORY DATA ANALYSIS (EDA)---------------------------
-- the Eda has no definite agenda, its about exploring the dataset and looking and providing insight at everyday

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_working2;

SELECT 
    *
FROM
    layoffs_working2
WHERE
    percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT 
    company, SUM(total_laid_off) AS 'TOTAL LAID OFF'
FROM
    layoffs_working2
GROUP BY 1
ORDER BY 2 DESC;


SELECT 
    MIN(`date`), MAX(`date`)
FROM
    layoffs_working2;

SELECT 
    industry, SUM(total_laid_off)
FROM
    layoffs_working2
GROUP BY industry
ORDER BY 2 DESC;

SELECT 
    country, SUM(total_laid_off)
FROM
    layoffs_working2
GROUP BY 1
ORDER BY 2 DESC;

SELECT 
    YEAR(`date`), SUM(total_laid_off)
FROM
    layoffs_working2
GROUP BY 1
ORDER BY 1 DESC;

SELECT 
    stage, SUM(total_laid_off)
FROM
    layoffs_working2
GROUP BY 1
ORDER BY 2 DESC;

-- TOTAL LAID OFF BY MONTHS
SELECT 
    SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM
    layoffs_working2
WHERE
    SUBSTRING(`date`, 1, 7) IS NOT NULL
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
SELECT 
    company, YEAR(`date`), SUM(total_laid_off)
FROM
    layoffs_working2
GROUP BY company , YEAR(`date`)
ORDER BY 3 DESC;

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


SELECT 
    industry, SUM(total_laid_off) AS total_laid_off
FROM
    layoffs_working2
GROUP BY industry
ORDER BY total_laid_off DESC;

/* QUESTIONS
2. Which countries have experienced the highest percentage of layoffs */

SELECT 
    country, AVG(percentage_laid_off) AS avg_laid_off
FROM
    layoffs_working2
GROUP BY country
ORDER BY avg_laid_off DESC;

/* QUESTIONS
3. Has the rate of layoffs increased or decreased over time */

SELECT 
    YEAR(date) AS year, SUM(total_laid_off) AS total_laid_off
FROM
    layoffs_working2
WHERE
    YEAR(date) IS NOT NULL
GROUP BY year
ORDER BY year ASC;

/* QUESTIONS
4. Are there any correlations between company size (based on funding raised) and the likelihood of layoffs? */

SELECT 
    AVG(percentage_laid_off) AS avg_laid_off,
    CASE
        WHEN funds_raised_millions < 20000 THEN 'Small Company'
        WHEN funds_raised_millions BETWEEN 20000 AND 60000 THEN 'Medium Company'
        ELSE 'Large Company'
    END AS Company_size
FROM
    layoffs_working2
GROUP BY 2
ORDER BY 2 ASC;


/* QUESTIONS
5. Which companies have laid off the most employees? */

SELECT 
    company, SUM(total_laid_off) AS total_laid_off
FROM
    layoffs_working2
GROUP BY company
ORDER BY total_laid_off DESC;

/* QUESTIONS
6. How have layoffs affected companies in different stages of growth? */

SELECT 
    stage, AVG(percentage_laid_off) AS avg_laid_off
FROM
    layoffs_working2
GROUP BY stage
ORDER BY avg_laid_off DESC;

