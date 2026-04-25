import process from 'node:process';
import fs from 'node:fs';
import yaml from 'yaml';
import { tally } from './tally.mjs';
import { database } from './database.mjs';
import { logger } from './logger.mjs';
let isSyncRunning = false;
let lastMasterAlterId = 0;
let lastTransactionAlterId = 0;
function parseCommandlineOptions() {
    let retval = new Map();
    try {
        let lstArgs = process.argv;
        if (lstArgs.length > 2 && lstArgs.length % 2 == 0)
            for (let i = 2; i < lstArgs.length; i += 2) {
                let argName = lstArgs[i];
                let argValue = lstArgs[i + 1];
                if (/^--\w+-\w+$/g.test(argName))
                    retval.set(argName.substr(2), argValue);
            }
    }
    catch (err) {
        logger.logError('index.substituteTDLParameters()', err);
    }
    return retval;
}
function invokeImport(forceTruncate = false) {
    return new Promise(async (resolve) => {
        try {
            isSyncRunning = true;
            // Check if daily truncate is needed (only for incremental sync)
            if (tally.config.sync === 'incremental' && forceTruncate) {
                // Reopen connection pool if needed (it may have been closed by previous sync)
                await database.openConnectionPool();
                const tableNames = getAllTableNames();
                await database.truncateAllTables(tableNames);
            }
            await tally.importData();
            logger.logMessage('Import completed successfully [%s]', new Date().toLocaleString());
        }
        catch (err) {
            logger.logMessage('Error in importing data\r\nPlease check error-log.txt file for detailed errors [%s]', new Date().toLocaleString());
        }
        finally {
            isSyncRunning = false;
            resolve();
        }
    });
}
function getAllTableNames() {
    try {
        const yamlContent = fs.readFileSync(tally.config.definition, 'utf8');
        const config = yaml.parse(yamlContent);
        const tableNames = [];
        // Add special tables
        tableNames.push('_diff', '_delete', '_vchnumber', 'config');
        // Add master tables
        if (config.master) {
            for (const table of config.master) {
                tableNames.push(table.name);
            }
        }
        // Add transaction tables
        if (config.transaction) {
            for (const table of config.transaction) {
                tableNames.push(table.name);
            }
        }
        return tableNames;
    }
    catch (err) {
        logger.logError('index.getAllTableNames()', err);
        return [];
    }
}
//Update commandline overrides to configuration options
let cmdConfig = parseCommandlineOptions();
database.updateCommandlineConfig(cmdConfig);
tally.updateCommandlineConfig(cmdConfig);
// Setup daily truncate timer (checks every minute)
if (tally.config.sync === 'incremental' && database.config.daily_truncate_time) {
    setInterval(async () => {
        try {
            if (database.shouldTruncateToday()) {
                logger.logMessage('Daily truncate time reached, waiting for current sync to complete [%s]', new Date().toLocaleString());
                // Wait for current sync to finish if running (no timeout - wait indefinitely)
                while (isSyncRunning) {
                    await new Promise(r => setTimeout(r, 1000)); // check every second
                }
                logger.logMessage('Starting daily truncate [%s]', new Date().toLocaleString());
                // Force truncate and sync
                await invokeImport(true);
            }
        }
        catch (err) {
            logger.logError('Daily truncate timer', err);
            logger.logMessage('Daily truncate failed, will retry at next scheduled time [%s]', new Date().toLocaleString());
        }
    }, 60000); // check every minute
}
if (tally.config.frequency <= 0) { // on-demand sync
    await invokeImport();
    logger.closeStreams();
}
else { // continuous sync
    const triggerImport = async () => {
        try {
            // skip if sync is already running (wait for next trigger)
            if (!isSyncRunning) {
                await tally.updateLastAlterId();
                let isDataChanged = !(lastMasterAlterId == tally.lastAlterIdMaster && lastTransactionAlterId == tally.lastAlterIdTransaction);
                if (isDataChanged) { // process only if data is changed
                    //update local variable copy of last alter ID
                    lastMasterAlterId = tally.lastAlterIdMaster;
                    lastTransactionAlterId = tally.lastAlterIdTransaction;
                    await invokeImport();
                }
                else {
                    logger.logMessage('No change in Tally data found [%s]', new Date().toLocaleString());
                }
            }
        }
        catch (err) {
            if (typeof err == 'string' && err.endsWith('is closed in Tally')) {
                logger.logMessage(err + ' [%s]', new Date().toLocaleString());
            }
            else {
                throw err;
            }
        }
    };
    if (!tally.config.company) { // do not process continuous sync for blank company
        logger.logMessage('Continuous sync requires Tally company name to be specified in config.json');
    }
    else { // go ahead with continuous sync
        setInterval(async () => await triggerImport(), tally.config.frequency * 60000);
        await triggerImport();
    }
}
//# sourceMappingURL=index.mjs.map