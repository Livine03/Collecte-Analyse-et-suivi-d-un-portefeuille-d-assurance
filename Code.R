library(DBI)
R.version.string
library(RPostgres)

# connection a la base de donnees

con <- dbConnect(
  RPostgres::Postgres(),
  dbname = "Data",        # Nom de la base de données
  host = "localhost",     # Adresse du serveur
  port = 5432,            # Port de connexion
  user = "postgres",      # Nom d'utilisateur
  password = "root"       # Mot de passe
)
tables <- dbListTables(con)
tables
data <- dbGetQuery(con, "SELECT * FROM data_client")
print(data)
# afficher le  type de chaque colonne
 sapply(data,class)
 
# verifions les donnees manquantes
 sapply (data,function(x) sum(is.na(x)))
 
# afficher les valeurs  dupliquees
data[duplicated(data),]

# afficher les valeurs unique des colonnes
unique(data$Produit)
unique(data$Région)
unique(data$Statut)


# analyse generale du dataset
summary(data)

# on va grouper les differentes colonnes
# chargement de la librairie
library(dplyr)

# changeons le type de la colonne versement_annuelle
 
data$Versements_Annuel <- as.numeric(data$Versements_Annuel)

#Grouper par produit et calculer la somme des versements
data %>%
  group_by(Produit) %>%
  summarise(Somme = sum(Versements_Annuel))


# Grouper le Statut par versement

data %>%
  group_by(Statut) %>%
  summarise(total=sum(Versements_Annuel))

# determinons  la région qui fait plus de chiffre d'affaire

data %>%
  group_by(Région) %>%
  summarise(somme = sum(Versements_Annuel))


# importons la base de donnees transaction

BD <- dbGetQuery(con, "SELECT * FROM data_transaction" )
BD

# analyse generale du datase transaction
summary(BD)

# verifions si le dataset contient des donnees manquantes
sapply (BD,function(x) sum(is.na(x)))

# afficher le  type de chaque colonne
sapply(BD,class)
 
# afficher les valeurs  dupliquees
BD[duplicated(BD),]

# Jointure des 2 tables

BD_final <- inner_join(data, BD, by = "id_client")
BD_final

# transformons  ce dataframe en  un fichier csv

fichier <- write.csv(BD_final,file = "C:/Users/Livine/Desktop/Dossier_R/ BD_final.csv",row.names = TRUE)
fichier1 <- write.csv(data,file = "C:/Users/Livine/Desktop/Dossier_R/ dataset_clients.csv",row.names = TRUE)
fichier2 <- write.csv(BD,file = "C:/Users/Livine/Desktop/Dossier_R/ dataset_transactions.csv",row.names = TRUE)
