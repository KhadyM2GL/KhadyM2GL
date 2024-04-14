-- Charger les données depuis le fichier CSV
sales = LOAD 'hdfs://localhost:9000/C:/Users/HP/Documents/Master 2/Big_Data/sales_data.csv' USING PigStorage(',') AS (
    user_id:chararray,
    product_id:chararray,
    category:chararray,
    price:float,
    quantity:int,
    timestamp:chararray
);

-- Filtrer les ventes dans la catégorie "Electronics"
electronics_sales = FILTER sales BY category == 'Electronics';

-- Calculer le montant total des ventes par utilisateur
user_sales = FOREACH (GROUP electronics_sales BY user_id) {
    total_sales = SUM(electronics_sales.price * electronics_sales.quantity);
    GENERATE group AS user_id, total_sales AS total_sales;
};

-- Classer les utilisateurs en fonction du montant total des ventes (descendant)
sorted_users = ORDER user_sales BY total_sales DESC;

-- Définir le chemin de sortie dans HDFS (ne pas utiliser un chemin local)
output_path = 'hdfs://localhost:9000/C:/Users/HP/Documents/Master 2/Big_Data/output';

-- Stocker les résultats dans un fichier de sortie sur HDFS
STORE sorted_users INTO $output_path USING PigStorage(',');

-- Afficher un message de confirmation
DESCRIBE sorted_users;
