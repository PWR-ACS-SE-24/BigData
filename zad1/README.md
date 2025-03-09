# Przetwarzanie dużych zbiorów danych (PDZB)

<div align="center">
Zespół: <br/> <b>Tomasz Chojnacki (260365), Kamila Iwańska (253027), Jakub Zehner (260285)</b>
</div>

## Zadanie 1 - Założenia projektu

Tematem naszego projektu jest **analiza wpływu sytuacji geopolitycznej oraz warunków atmosferycznych na charakterystykę najczęściej odtwarzanych utworów muzycznych w serwisie Spotify**. W ramach projektu planujemy zebrać dane dotyczące list przebojów w różnych krajach w poszczególnych dniach, wzbogacić je o cechy dźwiękowe oraz metadane utworów, a następnie zestawić te utwory z danymi dotyczącymi sytuacji w kraju (np. PKB, wskaźniki społeczne, emisja CO2, itp.) oraz warunków atmosferycznych (temperatura, wilgotność, ciśnienie, itp.).

### Dane źródłowe

Wybraliśmy cztery źródła danych, w tym trzy statyczne o różnych rozmiarach i formatach oraz jedno dynamiczne, pobierające dane z API. Wszystkie zbiory danych są publicznie dostępne oraz zostały załączone oraz opisane poniżej.

#### Spotify Charts (Kaggle)

**Źródło:** [kaggle.com/datasets/dhruvildave/spotify-charts](https://www.kaggle.com/datasets/dhruvildave/spotify-charts)  
**Typ:** statyczne  
**Rozmiar:** 3 480 MB  
**Format:** CSV  

Zbiór danych zawiera 26.2 miliona rekordów oraz 9 kolumn, takich jak: tytuł utworu, wykonawca, lista przebojów i zajęte miejsce, data, region.

#### Spotify Web API (Spotify)

**Źródło:** [developer.spotify.com/documentation/web-api](https://developer.spotify.com/documentation/web-api)  
**Typ:** dynamiczne  
**Rozmiar:** —  
**Format:** JSON  

API oferuje dostęp do cech audio utworów (np. tempo, tonacja, głośność, taneczność, energia, itp.) oraz metadanych (np. nazwa, wykonawca, rok wydania, itp.). Do korzystania z API wymagane jest podanie klucza, zezwalającego na około 25 zapytań na sekundę. Jako że będziemy analizować tylko popularne utwory (takie, które znalazły się na liście przebojów w co najmniej jednym kraju) nie powinno to stanowić problemu.

#### The Weather Dataset (Kaggle)

**Źródło:** [kaggle.com/datasets/guillemservera/global-daily-climate-data](https://www.kaggle.com/datasets/guillemservera/global-daily-climate-data)  
**Typ:** statyczne  
**Rozmiar:** 245 MB  
**Format:** PARQUET  

Zbiór danych zawiera ponad 50 milionów rekordów zawierających pomiary warunków atmosferycznych (temperatura, wilgotność, ciśnienie, itp.) z różnych stacji meteorologicznych na całym świecie. Dane obejmują zakres od 1833 do 2023 roku oraz 1235 miast z 214 krajów.

#### World Development Indicators (World Bank Group)

**Źródło:** [databank.worldbank.org/source/world-development-indicators](https://databank.worldbank.org/source/world-development-indicators)  
**Typ:** statyczne  
**Rozmiar:** około 1 000 MB  
**Format:** CSV/XLSX  

Strona umożliwia eksport danych CSV/XLSX dla różnych wskaźników ekonomicznych, społecznych, środowiskowych, itp. z różnych krajów na świecie. Dane obejmują zakres od 1960 do 2023 roku, przy czym nas interesuje podzbiór lat zgodny z zakresem zbioru Spotify Charts.

### Dane wynikowe

Dane wynikowe są zdenormalizowane i chcemy w nich przedstawić następujące informacje: kraj, dzień, pogoda (średnie wskaźniki w skali kraju), sytuacja geopolityczna (wybrane istotne wskaźniki), uśrednione cechy dźwiękowe najpopularniejszych utworów w danym kraju w danym dniu.

Dane te mogą zostać wykorzystane do analizy zależności cech utworów od wspomianych wskaźników lub wytrenowania modelu predykcyjnego sztucznej inteligencji, który będzie proponował użytkownikowi utwory muzyczne na podstawie sytuacji geopolitycznej oraz warunków atmosferycznych w danym kraju danego dnia.
