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

