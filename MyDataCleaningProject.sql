-- DATA CLEANING

SELECT * FROM layoffs;

-- CREATING A STAGING TABLE

CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT INTO layoffs_staging 
SELECT * FROM layoffs;

SELECT * FROM layoffs_staging;

-- 1. REMOVING DUPLICATES
WITH Duplicate_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, percentage_laid_off,stage,country, funds_raised_millions,industry,total_laid_off,`date`) AS row_num
FROM layoffs_staging
)
SELECT * FROM Duplicate_CTE
WHERE row_num > 1;

SELECT * FROM layoffs_staging
WHERE company  = 'Casper';

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, percentage_laid_off,stage,country, funds_raised_millions,industry,total_laid_off,`date`) AS row_num
FROM layoffs_staging;

DELETE FROM layoffs_staging2
WHERE row_num >1;

SELECT * FROM layoffs_staging2
WHERE row_num >1;

SELECT * FROM layoffs_staging2;
-- 2. STANDARDIZE DATA

SELECT DISTINCT TRIM(company) 
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry 
FROM layoffs_staging2
ORDER BY 1;

SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


SELECT DISTINCT location 
FROM layoffs_staging2;

SELECT DISTINCT country 
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2 
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT DISTINCT country 
FROM layoffs_staging2
ORDER BY 1;

SELECT `date`, str_to_date(`date`,'%m/%d/%Y') FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. NULL OR BLANK VALUES

SELECT * FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

SELECT * FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND
t2.industry IS NOT NULL;

SELECT * FROM layoffs_staging2
WHERE company = 'Airbnb';

-- 4. REMOVING COLUMNS OR ROWS

SELECT * 
FROM layoffs_staging2 
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2 
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging2 ;