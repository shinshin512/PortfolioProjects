Select *
From PortfolioProject..CovidDeaths
order by 3,4

--Select *
--From PortfolioProject.dbo.CovidVaccinations
--order by 3,4
--Select Data that we are going to be using

Select Location, date, total_cases, new_cases, total_deaths, population
From PortfolioProject..CovidDeaths
order by 1

--Looking at Total Cases vs Total Deaths
--Show likelihood of dying if u track the death percentage in interested country
Select Location, date, total_cases, new_cases, total_deaths, (Convert(float, total_deaths)/Nullif(Convert(float, total_cases),0))*100 AS DeathPercentage
From PortfolioProject..CovidDeaths
Where location like '%States%'
order by 1

--total cases vs population
--show the percentage of infected population in interested country
Select Location, date, total_cases, population, (Convert(float, total_cases)/Nullif(Convert(float, population),0))*100 AS PercentPopulationInfected
From PortfolioProject..CovidDeaths
Where location like '%States%'
order by 1

--the highest infectedPercentage country generally
Select Location, date, total_cases, population, (Convert(float, total_cases)/Nullif(Convert(float, population),0))*100 AS PercentPopulationInfected
From PortfolioProject..CovidDeaths
order by  PercentPopulationInfected DESC
--the highest infection rate of each country
--Highest affection count is highest affection count per location
Select Location, population, continent, Max(total_cases) AS HighestInfectionCount, Max(Convert(float, total_cases)/Nullif(Convert(float, population),0))*100 AS PercentPopulationInfected
From PortfolioProject..CovidDeaths
Group by location, population, continent
order by PercentPopulationInfected DESC
--?the population and location is mostly the same in every country. Why do we need to use 2 group by: population and country

--showing countries with highest death count per population
Select Location, Max(Cast(Total_deaths as int)) AS TotalDeathCount
From PortfolioProject..CovidDeaths
Where COALESCE(continent, '') <> ''
Group by location
order by TotalDeathCount DESC

--analyse by continent
--showing the continents with the highest death count per population
--sth is worong here
Select continent, Max(Cast(Total_deaths as int)) AS TotalDeathCount
From PortfolioProject..CovidDeaths
Where COALESCE(continent, '') <> ''
Group by continent
order by TotalDeathCount DESC
--this one is more rational
--Select location, Max(Cast(Total_deaths as int)) AS TotalDeathCount
--From PortfolioProject..CovidDeaths
--Where COALESCE(continent, '') = ''
--Group by location
--order by TotalDeathCount DESC

--GLOBAL NUMBERS
--number of cases per date globally
Select date, SUM(cast(new_cases  as int)) AS total_cases, SUM(Convert(float, new_deaths)) as total_death, SUM(Convert(float, new_deaths))/SUM(Nullif(Convert(float, new_cases),0))*100 as DeathPercentage--, total_cases,  total_deaths, (Convert(float, total_deaths)/Nullif(Convert(float, total_cases),0))*100 AS DeathPercentage
From PortfolioProject..CovidDeaths
Where COALESCE(continent, '') <> ''
Group by date
--number of cases globally
Select SUM(cast(new_cases  as int)) AS total_cases, SUM(Convert(float, new_deaths)) as total_death, SUM(Convert(float, new_deaths))/SUM(Nullif(Convert(float, new_cases),0))*100 as DeathPercentage--, total_cases,  total_deaths, (Convert(float, total_deaths)/Nullif(Convert(float, total_cases),0))*100 AS DeathPercentage
From PortfolioProject..CovidDeaths
Where COALESCE(continent, '') <> ''


--another table, CovidVaccinations

---join the table
SELECT *
FROM PortfolioProject..CovidDeaths as dea
--can also write as; PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations as vac
	On dea.location = vac.location
	and dea.date=vac.date

----Looking at Total population vs Vaccinations
--new vaccination per date in each location
SELECT dea.continent, dea.location, dea.date, population, vac.new_vaccinations
--date must be specified the table we want
--for some colname that stay in only either table can do both spicify or not
FROM PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
	On dea.location=vac.location
	and dea.date=vac.date
Where COALESCE(dea.continent, '') <> ''
ORDER BY 1,2,3
----total number of vaccination in each date
--Use CTE
With PopvsVac (continent, location, date, population, new_vaccinations,RollingPeopleVaccinated)
--in CTE, the #of colname in parentheses must= #colname of SELECT 
as
(
SELECT dea.continent, dea.location, dea.date, population, vac.new_vaccinations
, SUM(CONVERT(float, vac.new_vaccinations)) OVER (partition by dea.location ORDER by dea.location, dea.date) 
as RollingPeopleVaccinated
--RollingPeopleVaccinated colname cant be use to do any operations here. If we want to do so, we need to create CTE
FROM PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
	On dea.location=vac.location
	and dea.date=vac.date
Where COALESCE(dea.continent, '') <> ''
--ORDER BY 2,3
)
--with the use of CTE, now we can use rollingpeoplevaccinated to do operation
SELECT *, (RollingPeopleVaccinated/NULLIF(population,0))*100 as VaccinatedPeoplePercentage
FROM PopvsVac

--Use Temp Table, instead of CTE
Create Table #PercentPopulationVaccinated
(
Continent, nvarchar(255),
Location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
RollingPeopleVaccinated numeric
)
Insert into #PercentPopulationVaccinated
SELECT dea.continent, dea.location, dea.date, population, vac.new_vaccinations
, SUM(CONVERT(float, vac.new_vaccinations)) OVER (partition by dea.location ORDER by dea.location, dea.date) 
as RollingPeopleVaccinated
--RollingPeopleVaccinated colname cant be use to do any operations here. If we want to do so, we need to create CTE
FROM PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
	On dea.location=vac.location
	and dea.date=vac.date
Where COALESCE(dea.continent, '') <> ''