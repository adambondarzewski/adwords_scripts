function create_lock() {
  var drive_dir_daily     = prepare_drive_dir(DriveApp, DRIVE_DIR_DAILY_NAME);
  var drive_dir_yesturday = prepare_drive_dir(drive_dir_daily, prepare_drive_name(start_date_report, end_date_report));
  var result = drive_dir_yesturday.createFile('lock', '');
  Logger.log('Lock created');
  return true;
}


function delete_lock() {
  var drive_dir_daily     = prepare_drive_dir(DriveApp, DRIVE_DIR_DAILY_NAME);
  var drive_dir_yesturday = prepare_drive_dir(drive_dir_daily, prepare_drive_name(start_date_report, end_date_report));
  lock_file = getFile(drive_dir_yesturday, 'lock')
  drive_dir_yesturday.removeFile(lock_file);
  Logger.log('Lock deleted');
}


function day_dir_locked() {
  var drive_dir_daily     = prepare_drive_dir(DriveApp, DRIVE_DIR_DAILY_NAME)
  var drive_dir_yesturday = prepare_drive_dir(drive_dir_daily, prepare_drive_name(start_date_report, end_date_report))

  if(file_exists(drive_dir_yesturday, 'lock')){
    return true;
  }
}


function get_previous_day_str(days_before){
    var MILLIS_PER_DAY = 1000 * 60 * 60 * 24;
  
  return Utilities.formatDate(new Date(new Date().getTime() - 1 * MILLIS_PER_DAY), TIMEZONE, 'yyyyMMdd');

}

function get_yesturday_str(){
 return get_previous_day_str(1);
}

function get_7_days_ago_str(){
 return get_previous_day_str(7);
}

function prepare_drive_name(start_date, end_date) {
  return  start_date + '-' + end_date;
}

function create_accounts_list_file(){
    var drive_dir_daily     = prepare_drive_dir(DriveApp, DRIVE_DIR_DAILY_NAME)
    var drive_dir_yesturday = prepare_drive_dir(drive_dir_daily, prepare_drive_name(start_date_report, end_date_report))

    if(file_exists(drive_dir_yesturday, FNAME_ACCOUNTS_LIST)){
        Logger.log('Accounts list file exists: '+drive_dir_yesturday+'/'+FNAME_ACCOUNTS_LIST);
        return true;
    }
    Logger.log('Creating accounts list file: '+drive_dir_yesturday+'/'+FNAME_ACCOUNTS_LIST);
    var accounts_data_tmp = {};
    var accountIterator = MccApp.accounts()
        .withCondition("Impressions > 0")
        .orderBy('Clicks ASC')
        //.withIds(['513-052-7996'])
        .forDateRange(start_date_report, end_date_report)
        //.forDateRange("YESTERDAY")
        .get();


    while (accountIterator.hasNext()) {
        var account = accountIterator.next();
        accounts_data_tmp[account.getCustomerId()] = {
            'name'          : account.getName(),
            'curr'          : account.getCurrencyCode(),
            'downloaded'    : 0,
            'status'        : '',
            'message'       : ''
        };
    }

    var accounts_json = JSON.stringify(accounts_data_tmp)
    drive_dir_yesturday.createFile(FNAME_ACCOUNTS_LIST, accounts_json, MimeType.JSON);
    Logger.log('Accounts list created: '+drive_dir_yesturday+'/'+FNAME_ACCOUNTS_LIST);
    return true;
}



function download_yesturday_raports() {

    var drive_dir_daily     = prepare_drive_dir(DriveApp, DRIVE_DIR_DAILY_NAME)
    var drive_dir_yesturday = prepare_drive_dir(drive_dir_daily, prepare_drive_name(start_date_report, end_date_report))
    var accounts_data       = readJSONFile(drive_dir_yesturday, FNAME_ACCOUNTS_LIST)
    var accids_50           = []
    var skipped             = 0
    for (var accid in accounts_data) {
        if(accounts_data[accid]['downloaded'] == 0){
            accids_50.push(accid)
            if(accids_50.length == PARALLEL_THREADS){
                break;
            }
        }else{
            skipped += 1
        }
    }
    Logger.log('Skipping '+skipped+' already downloaded accounts.');
    download_accounts(accids_50)
}


function download_accounts(accids){
    if( accids.length == 0 ){
        Logger.log('No accounts to download left.');
      delete_lock();
        return true;
    }
    MccApp.accounts().withIds(accids).executeInParallel('download_account_thread', 'reportOnResults');
    return true;
}

function download_account_thread() {
    var account         = AdWordsApp.currentAccount();
    var current_accid   = account.getCustomerId();
    var msg             = '';
    Logger.log('Processing: '+account.getName())+' START';

    var drive_dir_daily     = prepare_drive_dir(DriveApp, DRIVE_DIR_DAILY_NAME)
    var drive_dir_yesturday = prepare_drive_dir(drive_dir_daily, prepare_drive_name(start_date_report, end_date_report))

    for (var i = 0; i < CONFIG.REPORTS.length; i++) {
        var reportConfig = CONFIG.REPORTS[i];
        var fileNameBeggining = reportConfig.NAME + '_' + current_accid; //only for part 0, other parts should be managed

        if( file_exists(drive_dir_yesturday, fileNameBeggining + '_part_0')){
            Logger.log('Report file already exists');
            msg += ' Report file already exists';
        }else{
            //Logger.log('Running report %s for account %s', reportConfig.NAME, current_accid);
            // Get data as csv
            var csvData = retrieveAdwordsReport(reportConfig, current_accid);


            for ( i=0; i<= (csvData.length - 1); i++) {
              
              var fileName = fileNameBeggining + '_part_' + i;
              var blob = Utilities.newBlob(csvData[i], "application/zip", fileName + '.csv');
  
              var zip = Utilities.zip([blob], fileName + '.zip');          
              
              drive_dir_yesturday.createFile(zip);
            
              msg += ' Report Exported to:' + drive_dir_yesturday+ ' for report ' + fileName;
        }
      }
    }
    Logger.log('Processing: '+account.getName())+' DONE';
    return msg;
}

/**
 * Retrieves AdWords data as csv and formats any fields
 * to BigQuery expected format.
 *
 * @param {Object} reportConfig Report configuration including report name,
 *    conditions, and fields.
 * @param {string} accountId Account Id to run reports.
 *
 * @return {string} csvData Report in csv format.
 */
function retrieveAdwordsReport(reportConfig, accountId) {
  var fieldNames = Object.keys(reportConfig.FIELDS);
  var report = AdWordsApp.report(
    'SELECT ' + fieldNames.join(',') +
    ' FROM ' + reportConfig.NAME + ' ' + reportConfig.CONDITIONS +
    ' DURING ' + CONFIG.DEFAULT_DATE_RANGE,
    {apiVersion: CONFIG.API_VERSION});
  var rows = report.rows();
  var csvRows = [];
  
  // Header row
  csvRows.push('AccountId,'+fieldNames.join(','));
  
  var Array_all = []; // array of arrays
  var count = 0;

  // Iterate over each row.
  while (rows.hasNext()) {
    count++;
    var row = rows.next();
    var csvRow = [];
    csvRow.push(accountId);

    for (var i = 0; i < fieldNames.length; i++) {
      var fieldName = fieldNames[i];
      var fieldValue = row[fieldName].toString();
      var fieldType = reportConfig.FIELDS[fieldName];
      // Strip off % and perform any other formatting here.
      if (fieldType == 'FLOAT' || fieldType == 'INTEGER') {
        if (fieldValue.charAt(fieldValue.length - 1) == '%') {
          fieldValue = fieldValue.substring(0, fieldValue.length - 1);
        }
        fieldValue = fieldValue.replace(/,/g,'');
      }
      // Add double quotes to any string values.
      if (fieldType == 'STRING') {
        fieldValue = fieldValue.replace(/"/g, '""');
        fieldValue = '"' + fieldValue + '"';
      }
      if (fieldType == 'LIST') {
        fieldValue = fieldValue.replace(/\[/g, '');
        fieldValue = fieldValue.replace(/\]/g, '');
      }
      csvRow.push(fieldValue);
    }
    csvRows.push(csvRow.join(','));
    
      if ( count % ROWS_MAX == 0) {
       Logger.log(count + 'teraz podmianka')
       csvRows = csvRows.join('\n')
       Array_all = Array_all.concat(csvRows)
       var csvRows = []; // new instance of this containter
      // Header row
       csvRows.push('AccountId,'+fieldNames.join(','));
     }
  }
  
    // last part
  csvRows = csvRows.join('\n');
  Array_all = Array_all.concat(csvRows);
  
  Logger.log('Downloaded ' + reportConfig.NAME + ' for account ' + accountId + ' with ' + csvRows.length + ' rows.');
  return Array_all;

}

//And this loads that stored file and converts it to an object
function readJSONFile(drive_dir_obj, fileName) {
  var file = getFile(drive_dir_obj, fileName, false);
  if( file == null){
      return null;
  }
  var fileData = file.getBlob().getDataAsString();
  if(fileData) {
    return JSON.parse(fileData);
  } else {
    return null;
  }
}


function getFile(drive_dir_obj, fileName) {
    var maxRetries = 3;
    var errors = [];
    while(maxRetries > 0) {
        try {
            var fileIter = drive_dir_obj.getFilesByName(fileName);
            if(!fileIter.hasNext()) {
                Logger.log('Could not find file: '+fileName+' on Google Drive.');
                return null;
            } else {
                return fileIter.next();
            }
        } catch(e) {
            Logger.log('Exception while getting file: '+fileName+', retring ...');
            errors.push(e);
            maxRetries--;
            Utilities.sleep(1000);
        }
    }
    if(maxRetries === 0) {
        throw errors.join('. ');
    }
}

function store_accounts_list_file(accounts_data){
    Logger.log('store_accounts_list_file()');
    var drive_dir_daily     = prepare_drive_dir(DriveApp, DRIVE_DIR_DAILY_NAME)
    var drive_dir_yesturday = prepare_drive_dir(drive_dir_daily, prepare_drive_name(start_date_report, end_date_report))
    var accounts_json       = JSON.stringify(accounts_data)
    var accounts_file_iter  = drive_dir_yesturday.getFilesByName(FNAME_ACCOUNTS_LIST);

    if( accounts_file_iter.hasNext() ){
        var accounts_file = accounts_file_iter.next();
        accounts_file.setContent(accounts_json);
        Logger.log('Accounts list updated: '+drive_dir_yesturday+'/'+FNAME_ACCOUNTS_LIST);
    }else{
        drive_dir_yesturday.createFile(FNAME_ACCOUNTS_LIST, accounts_json, MimeType.JSON);
        Logger.log('Accounts list created: '+drive_dir_yesturday+'/'+FNAME_ACCOUNTS_LIST);
    }
    return true;
}


 
function reportOnResults(results) {
    Logger.log('Creating report from downloads');

    var drive_dir_daily     = prepare_drive_dir(DriveApp, DRIVE_DIR_DAILY_NAME)
    var drive_dir_yesturday = prepare_drive_dir(drive_dir_daily, prepare_drive_name(start_date_report, end_date_report))
    var accounts_data       = readJSONFile(drive_dir_yesturday, FNAME_ACCOUNTS_LIST)
    var errors_cnt          = 0


    for(var i in results) {
        var result = results[i];
        var current_accid = result.getCustomerId();

        if(result.getStatus() == 'OK') {
            accounts_data[current_accid]['downloaded']  = 1;
            accounts_data[current_accid]['status']      = 'OK';
            accounts_data[current_accid]['message']     = result.getReturnValue();

        } else {
            accounts_data[current_accid]['downloaded']  = 0;
            accounts_data[current_accid]['status']      = result.getStatus();
            accounts_data[current_accid]['message']     = result.getError();
            errors_cnt = errors_cnt + 1
            
            // removing partial output
            var drive_dir_daily     = prepare_drive_dir(DriveApp, DRIVE_DIR_DAILY_NAME);
            var drive_dir_yesturday = prepare_drive_dir(drive_dir_daily, prepare_drive_name(start_date_report, end_date_report));
            var files_to_remove = drive_dir_yesturday.searchFiles('title contains "' + current_accid + '"');
            
            // check
            while (files_to_remove.hasNext()) {
              var file = files_to_remove.next();
              Logger.log('File to remove:' + file.getName());
              drive_dir_yesturday.removeFile(file);
           }
        }
    }
    Logger.log('ExecuteInParallel generated '+errors_cnt+' errors');
    store_accounts_list_file(accounts_data);
  delete_lock();
}
 
// This function will send an email to each email in the
// NOTIFY list from the top of the script with the specific error
function notifyOfAccountsWithErrors(erroredAccounts) {
  if(!erroredAccounts || erroredAccounts.length == 0) { return; }
  if(typeof NOTIFY == 'undefined') { throw 'NOTIFY is not defined.'; }
  var subject = ' - Accounts with Errors ';
   
  var htmlBody = 'The following Accounts had errors on the last run.<br / >';
  htmlBody += 'Log in to AdWords: http://goo.gl/7mS6A';
  var body = htmlBody;
  htmlBody += '<br / ><br / >';
  htmlBody += '<table border="1" width="95%" style="border-collapse:collapse;">' +
              '<tr><td>Account Id</td><td>Error</td></tr>';
  for(var i in erroredAccounts) {
    htmlBody += '<tr><td>'+ erroredAccounts[i].customerId +
      '</td><td>' + erroredAccounts[i].error + '</td></tr>';
  }
  htmlBody += '</table>';
  // Remove this line to get rid of the link back to this site.
  htmlBody += '<br / ><br / ><a href = "http://www.freeadwordsscripts.com" >FreeAdWordsScripts.com</a>';
  var options = { htmlBody : htmlBody };
  for(var i in NOTIFY) {
    Logger.log('Sending email to: '+NOTIFY[i]+' with subject: '+subject);
    MailApp.sendEmail(NOTIFY[i], subject, body, options);
  }
}

