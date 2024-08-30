// Définition des identifiants pour BigQuery
var projectId = 'spatial-climate-434012-f3';  // ID du projet BigQuery
var datasetId = 'Dataset_88E';  // ID du dataset
var tableId = '88E_temp';  // ID de la table

// Fonction pour uploader les données de Google Sheets à BigQuery
function uploadDataToBigQuery() {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();  // Obtenir la feuille active
  var data = sheet.getDataRange().getValues();  // Récupérer toutes les données de la feuille
  var rows = [];
  var headers = data[0];  // Les en-têtes de colonnes

  // Boucle pour transformer les données en format BigQuery
  for (var i = 1; i < data.length; i++) {  // Commence après les en-têtes
    var row = {};
    for (var j = 0; j < headers.length; j++) {  // Pour chaque colonne
      var header = headers[j];
      var value = data[i][j];
      
      // Convertir les dates au format BigQuery
      if (header.startsWith('DTE_') || header === 'DTE_CREATE') {
        if (value) {
          var date = new Date(value);
          row[header] = Utilities.formatDate(date, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
        } else {
          row[header] = null;  // Mettre `null` pour les valeurs manquantes
        }
      } else {
        row[header] = value;
      }
    }
    rows.push({ json: row });  // Ajouter la ligne transformée à la liste
  }

  // Préparer la requête d'insertion
  var insertAllRequest = { rows: rows };

  try {
    var response = BigQuery.Tabledata.insertAll(insertAllRequest, projectId, datasetId, tableId);
    if (response.insertErrors && response.insertErrors.length > 0) {
      Logger.log('Errors during insert: ' + JSON.stringify(response.insertErrors));  // Log les erreurs
    } else {
      Logger.log('Data inserted successfully.');  // Log succès
    }
  } catch (err) {
    Logger.log('Error during BigQuery insert: ' + err.toString());  // Log les erreurs en cas d'échec
  }
}

// Fonction pour mettre à jour des champs spécifiques dans BigQuery
function updateBigQueryFields() {
  // Exemple de mise à jour : changer certains champs pour des conditions spécifiques
  var updates = [
    {
      condition: "state = 'CHECKED-IN' AND src = 'Expedia'", // Condition pour trouver les enregistrements à mettre à jour
      updates: {
        state: "'CANCELED'",  // Nouvelle valeur pour "state"
        src: "'manual'",  // Nouvelle valeur pour "src"
        dte_canceled: "'2024-08-01'"  // Nouvelle valeur pour "dte_canceled"
      }
    }
    // Ajouter d'autres mises à jour si nécessaire
  ];

  updates.forEach(function(update) {
    var setClause = Object.keys(update.updates).map(function(field) {
      return field + ' = ' + update.updates[field];  // Préparer la clause SET
    }).join(', ');

    var query = "UPDATE `" + projectId + "." + datasetId + "." + tableId + "` " +
                "SET " + setClause + " " +
                "WHERE " + update.condition;  // Construire la requête SQL

    executeBigQueryUpdate(query);  // Exécuter la mise à jour
  });
}

// Fonction pour exécuter une requête UPDATE dans BigQuery
function executeBigQueryUpdate(query) {
  var request = {
    query: query,
    useLegacySql: false  // Utiliser SQL standard
  };

  try {
    var job = BigQuery.Jobs.insert({
      configuration: {
        query: {
          query: query,
          useLegacySql: false
        }
      }
    }, projectId);
    Logger.log('Update job created: ' + job.jobReference.jobId);  // Log ID du job

    // Attendre que le job se termine
    var jobId = job.jobReference.jobId;
    var status;
    do {
      Utilities.sleep(5000);  // Attendre 5 secondes
      var jobStatus = BigQuery.Jobs.get(projectId, jobId);
      status = jobStatus.status.state;
      Logger.log('Job status: ' + status);  // Log le statut du job
    } while (status !== 'DONE');

    if (jobStatus.status.errorResult) {
      Logger.log('Error: ' + jobStatus.status.errorResult.message);  // Log les erreurs si présentes
    } else {
      Logger.log('Update completed successfully.');  // Log succès
    }
  } catch (err) {
    Logger.log('Error during BigQuery update: ' + err.toString());  // Log les erreurs en cas d'échec
  }
}

// Fonction pour mettre à jour les données de Google Sheets depuis BigQuery
function updateSpreadsheetFromBigQuery() {
  var query = `SELECT * FROM \`${projectId}.${datasetId}.${tableId}\``;  // Requête SQL pour obtenir toutes les données

  var request = {
    query: query,
    useLegacySql: false  // Utiliser SQL standard
  };

  try {
    var queryResults = BigQuery.Jobs.query(request, projectId);
    var rows = queryResults.rows;

    if (!rows || rows.length === 0) {
      Logger.log('Aucune donnée trouvée.');  // Log si aucune donnée
      return;
    }

    var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();  // Obtenir la feuille active
    sheet.clear();  // Effacer tout le contenu existant

    var schema = queryResults.schema.fields;
    var headers = schema.map(field => field.name);  // Obtenir les en-têtes

    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);  // Écrire les en-têtes dans la feuille

    var data = rows.map(row => {
      return row.f.map((col, index) => {
        var fieldName = headers[index];
        var fieldValue = col.v;

        // Convertir les timestamps en format lisible
        if (fieldName.startsWith("DTE_") && fieldValue) {
          var timestampInSeconds = parseFloat(fieldValue);
          var date = new Date(timestampInSeconds * 1000);

          if (date.getTime() !== date.getTime()) {  // Vérifier si la date est invalide
            Logger.log('Erreur de conversion pour le champ ' + fieldName + ' avec la valeur ' + fieldValue);
            return fieldValue;
          } else {
            return Utilities.formatDate(date, Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
          }
        }

        return fieldValue;
      });
    });

    sheet.getRange(2, 1, data.length, headers.length).setValues(data);  // Insérer les données

    Logger.log('Les données ont été mises à jour avec succès.');  // Log succès

  } catch (error) {
    Logger.log('Erreur lors de la mise à jour des données : ' + error.toString());  // Log les erreurs
  }
}

// Fonction pour générer des statistiques mensuelles
function generateMonthlyStats() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();

  var statsSheet = spreadsheet.getSheetByName('Stats');
  if (!statsSheet) {
    statsSheet = spreadsheet.insertSheet('Stats');  // Créer la feuille Stats si elle n'existe pas
  } else {
    statsSheet.clear();  // Nettoyer la feuille Stats
  }

  var sheet = spreadsheet.getSheetByName('DATABASE');
  if (!sheet) {
    throw new Error("La feuille 'DATABASE' n'existe pas.");  // Erreur si la feuille DATABASE manque
  }

  var data = sheet.getDataRange().getValues();
  var headers = data[0];
  var rows = data.slice(1);

  var stats = {};

  rows.forEach(function(row, index) {
    var checkIn = new Date(row[headers.indexOf('DTE_CI')]);
    var checkOut = new Date(row[headers.indexOf('DTE_CO')]);
    var revenue = row[headers.indexOf('ACCOMODATION_AND_CLEANING_TTC')];
    var occupancyTax = row[headers.indexOf('CITY_TAX')];
    var ota = row[headers.indexOf('SRC')];
    var apartmentName = row[headers.indexOf('APT')];
    var apartmentType = getApartmentType(apartmentName);

    if (!ota || !apartmentName || !revenue || !occupancyTax) {
      Logger.log("Skipping row " + (index + 1) + " due to missing essential data.");  // Log si données manquantes
      return;
    }

    // Éclatement des réservations par mois
    while (checkIn < checkOut) {
      var month = checkIn.getMonth() + 1;
      var year = checkIn.getFullYear();
      var key = `${year}-${month}-${ota}-${apartmentName}`;
      
      if (!stats[key]) {
        stats[key] = {
          revenue: 0,
          occupancyTax: 0,
          nights: 0,
          reservations: 0,
          apartmentType: apartmentType,
          ota: ota,
          year: year,
          month: month,
          apartmentName: apartmentName
        };
      }
      
      stats[key].revenue += revenue / daysBetween(checkIn, checkOut);
      stats[key].occupancyTax += occupancyTax / daysBetween(checkIn, checkOut);
      stats[key].nights += 1;
      stats[key].reservations += 1;
      
      checkIn.setDate(checkIn.getDate() + 1);  // Passer au jour suivant
    }
  });

  // Préparer les résultats pour écriture dans la feuille Stats
  var result = [];
  for (var key in stats) {
    var stat = stats[key];
    stat.occupancyRate = stat.nights / getDaysInMonth(stat.year, stat.month);
    stat.ADR = stat.nights ? stat.revenue / stat.nights : 0;
    result.push([
      stat.year,
      stat.month,
      stat.ota,
      stat.apartmentName,
      stat.revenue.toFixed(2),
      stat.occupancyTax.toFixed(2),
      stat.nights,
      stat.reservations,
      (stat.occupancyRate * 100).toFixed(2) + '%',
      stat.ADR.toFixed(2)
    ]);
  }

  // Écrire les résultats dans la feuille Stats si des données sont présentes
  if (result.length > 0) {
    var headers = ["Year", "Month", "OTA", "Apartment Name", "Revenue", "Occupancy Tax", "Nights", "Reservations", "Occupancy Rate", "ADR"];
    statsSheet.appendRow(headers);
    statsSheet.getRange(2, 1, result.length, headers.length).setValues(result);
  } else {
    Logger.log("Aucune donnée n'a été trouvée pour les statistiques.");  // Log si pas de données
  }
}

// Fonction pour déterminer le type d'appartement
function getApartmentType(apartmentName) {
  if (!apartmentName) return 'Unknown';  // Retourner 'Unknown' si pas de nom
  apartmentName = apartmentName.toUpperCase();

  if (apartmentName.includes('T2+')) return 'T2+';
  if (apartmentName.includes('T3')) return 'T3';
  if (apartmentName.includes('T2')) return 'T2';
  
  return 'Unknown';  // Retourner 'Unknown' par défaut
}

// Fonction pour calculer le nombre de jours entre deux dates
function daysBetween(date1, date2) {
  return Math.ceil((date2 - date1) / (1000 * 60 * 60 * 24));  // Calculer la différence en jours
}

// Fonction pour obtenir le nombre de jours dans un mois
function getDaysInMonth(year, month) {
  return new Date(year, month, 0).getDate();  // Retourner le nombre de jours dans le mois
}
