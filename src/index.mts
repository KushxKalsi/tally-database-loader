import process from 'node:process';
import fs from 'node:fs';
import yaml from 'yaml';
import { tally } from './tally.mjs';
import { database } from './database.mjs';
import { logger } from './logger.mjs'

let isSyncRunning = false;
let isTruncatePending = false;
let lastMasterAlterId = 0;
let lastTransactionAlterId = 0;

function parseCommandlineOptions(): Map<string, string> {
    let retval = new Map<string, string>();
    try {
        let lstArgs = process.argv;

        if (lstArgs.length > 2 && lstArgs.length % 2 == 0)
            for (let i = 2; i < lstArgs.length; i += 2) {
                let argName = lstArgs[i];
                let argValue = lstArgs[i + 1];
                if (/^--\w+-\w+$/g.test(argName))
                    retval.set(argName.substr(2), argValue);
            }
    } catch (err) {
        logger.logError('index.substituteTDLParameters()', err);
    }
    return retval;
}

// resolves true if import succeeded, false if it failed (never rejects)
// caller is responsible for holding isSyncRunning flag
function invokeImport(forceTruncate: boolean = false): Promise<boolean> {
    return new Promise<boolean>(async (resolve) => {
        let isSuccess = false;
        try {
            // Clean up any leftover csv folder from a previously failed sync
            if (fs.existsSync('./csv')) {
                fs.rmSync('./csv', { recursive: true });
                logger.logMessage('Cleaned up leftover csv folder from previous sync [%s]', new Date().toLocaleString());
            }

            // Check if daily truncate is needed (only for incremental sync)
            if (tally.config.sync === 'incremental' && forceTruncate) {
                // Reopen connection pool if needed (it may have been closed by previous sync)
                await database.openConnectionPool();

                const tableNames = getAllTableNames();
                await database.truncateAllTables(tableNames);
            }

            await tally.importData();
            logger.logMessage('Import completed successfully [%s]', new Date().toLocaleString());
            isSuccess = true;
        }
        catch (err) {
            logger.logMessage('Error in importing data\r\nPlease check error-log.txt file for detailed errors [%s]', new Date().toLocaleString());
        }
        finally {
            resolve(isSuccess);
        }
    });
}

// Tables are emptied by truncate, so do it only when Tally is ready to send the data back
async function isTallyReachable(): Promise<boolean> {
    try {
        tally.lastAlterIdMaster = -1;
        await tally.updateLastAlterId(); // rejects if company is closed, leaves -1 if Tally is not responding
        return tally.lastAlterIdMaster >= 0;
    } catch (err) {
        return false;
    }
}

function getAllTableNames(): string[] {
    try {
        const yamlContent = fs.readFileSync(tally.config.definition, 'utf8');
        const config = yaml.parse(yamlContent);
        const tableNames: string[] = [];

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
    } catch (err) {
        logger.logError('index.getAllTableNames()', err);
        return [];
    }
}

//Update commandline overrides to configuration options
let cmdConfig = parseCommandlineOptions();
database.updateCommandlineConfig(cmdConfig);
tally.updateCommandlineConfig(cmdConfig);

// Setup daily truncate timer (checks every minute, daily_truncate_time can hold multiple times)
if (tally.config.sync === 'incremental' && tally.config.frequency > 0) {
    setInterval(async () => {
        if (isTruncatePending) { // previous check is still waiting / running
            return;
        }
        const dueTime = database.getDueTruncateTime();
        if (!dueTime) {
            return;
        }

        isTruncatePending = true;
        try {
            logger.logMessage('Daily truncate time %s reached, waiting for current sync to complete [%s]', dueTime, new Date().toLocaleString());

            // Wait for current sync to finish if running (no timeout - wait indefinitely)
            while (isSyncRunning) {
                await new Promise(r => setTimeout(r, 1000)); // check every second
            }

            isSyncRunning = true;
            try {
                if (!await isTallyReachable()) {
                    logger.logMessage('Tally is not ready, daily truncate postponed (will retry in a minute) [%s]', new Date().toLocaleString());
                    return;
                }

                logger.logMessage('Starting daily truncate [%s]', new Date().toLocaleString());

                // Force truncate and sync
                let isSuccess = await invokeImport(true);

                // Tables may be lying empty if sync failed after truncate, so retry sync (without truncate)
                for (let attempt = 1; !isSuccess && attempt <= 3; attempt++) {
                    logger.logMessage('Sync after daily truncate failed, retrying in a minute (attempt %d of 3) [%s]', attempt, new Date().toLocaleString());
                    await new Promise(r => setTimeout(r, 60000));
                    isSuccess = await invokeImport();
                }

                if (isSuccess) {
                    lastMasterAlterId = tally.lastAlterIdMaster;
                    lastTransactionAlterId = tally.lastAlterIdTransaction;
                }
                else { // force regular sync to try again on its next trigger
                    lastMasterAlterId = -2;
                    lastTransactionAlterId = -2;
                }
            } finally {
                isSyncRunning = false;
            }
        } catch (err) {
            logger.logError('Daily truncate timer', err);
            logger.logMessage('Daily truncate failed [%s]', new Date().toLocaleString());
        } finally {
            isTruncatePending = false;
        }
    }, 60000); // check every minute
}

if(tally.config.frequency <= 0) { // on-demand sync
    await invokeImport();
    logger.closeStreams();
}
else { // continuous sync
    const triggerImport = async () => {
        // skip if sync is already running or daily truncate is waiting for its turn (wait for next trigger)
        if(isSyncRunning || isTruncatePending) {
            return;
        }

        isSyncRunning = true;
        try {
            // data added / altered in Tally while sync was running is left out of that sync,
            // so follow it up with one more round right away instead of waiting for next trigger
            for(let round = 1; round <= 2; round++) {
                await tally.updateLastAlterId();

                let isDataChanged = !(lastMasterAlterId == tally.lastAlterIdMaster && lastTransactionAlterId == tally.lastAlterIdTransaction);
                if(!isDataChanged) { // process only if data is changed
                    if(round == 1) {
                        logger.logMessage('No change in Tally data found [%s]', new Date().toLocaleString());
                    }
                    break;
                }

                let masterAlterId = tally.lastAlterIdMaster;
                let transactionAlterId = tally.lastAlterIdTransaction;
                if(!await invokeImport()) {
                    break; // failed sync is retried on next trigger
                }
                //update local variable copy of last alter ID
                lastMasterAlterId = masterAlterId;
                lastTransactionAlterId = transactionAlterId;
            }
        } catch (err) {
            // do not let utility crash (e.g. company closed in Tally), next trigger will try again
            logger.logMessage('%s [%s]', typeof err == 'string' ? err : 'Error in checking Tally data for changes', new Date().toLocaleString());
        } finally {
            isSyncRunning = false;
        }
    }

    if(!tally.config.company) { // do not process continuous sync for blank company
        logger.logMessage('Continuous sync requires Tally company name to be specified in config.json');
    }
    else { // go ahead with continuous sync
        setInterval(async () => await triggerImport(), tally.config.frequency * 60000);
        await triggerImport();
    }
}