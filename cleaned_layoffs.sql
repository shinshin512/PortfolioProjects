-- Data Cleaning
SELECT *
FROM layoffs;

-- 1.Remove Duplicates
-- 2.Standardize data
-- 3.Null Values and Blank Values
-- 4.Remove any columns

-- create a copy of file layoffs in order to not meesing with the raw data
-- 1.only have the colnames
CREATE Table layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;
-- 2.insert the data to colnames 
INSERT layoffs_staging 
SELECT	 *
FROM layoffs;

-- remove duplicates
SELECT  *, 
	ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- CTE
WITH duplicate_cte AS
(
SELECT  *, 
	ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num>1;

CREATE TABLE `layoffs_staging2` (
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

-- this will copy only the column name/header
SELECT *
FROM layoffs_staging2;

-- u need to copy the data inside again
INSERT INTO layoffs_staging2
SELECT *,
	ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num>1;

SELECT *
FROM layoffs_staging2;
-- done deleting duplicates

-- standardizing data
UPDATE layoffs_staging2
SET company=TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER by 1;

SELECT *
FROM layoffs_staging2
WHERE industry 	LIKE 'Crypto%';
-- to update the name that we found it seems to be the same thing with crypto
UPDATE layoffs_staging2
SET industry='Crypto'
WHERE industry LIKE 'Crypto%';

-- find the possible issue in each column through eye scanning 1 by 1
SELECT DISTINCT(country)
FROM layoffs_staging2;
-- found that there is United States that is messy
-- UPDATE layoffs_staging2
-- SET country='United States'
-- WHERE country LIKE 'United States%'
-- ORDER BY 1
-- or another way of doing this
UPDATE layoffs_staging2
SET country=TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- change the date format
SELECT `date`, 
	STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date`=STR_TO_DATE(`date`, '%m/%d/%Y');
-- recheck it
SELECT `date`
FROM layoffs_staging2;
-- the date type now is still text
-- change the data type to date
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- remove null
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry=''
OR industry IS NULL;

UPDATE layoffs_staging2 t1
SET industry=NULL
WHERE industry='';

SELECT t1.company, t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry=t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;
-- for Belly's which has only one row, the industry cant be updated

-- check the laid_off
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
and percentage_laid_off IS NULL;
-- this data is cant be used in layoffs data, so it is fine to delete it
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
and percentage_laid_off IS NULL;

-- the row_num is not used now
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- the cleaned data
SELECT *
FROM layoffs_staging2;