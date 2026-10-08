import process from 'node:process';
import { tally } from './tally.mjs';
import { database } from './database.mjs';
import { logger } from './logger.mjs'

let isSyncRunning = false;
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
function invokeImport(): Promise<boolean> {
    return new Promise<boolean>(async (resolve) => {
        let isSuccess = false;
        try {
            isSyncRunning = true;
            await tally.importData();
            logger.logMessage('Import completed successfully [%s]', new Date().toLocaleString());
            isSuccess = true;
        }
        catch (err) {
            logger.logMessage('Error in importing data\r\nPlease check error-log.txt file for detailed errors [%s]', new Date().toLocaleString());
        }
        finally {
            isSyncRunning = false;
            resolve(isSuccess);
        }
    });
}

//Update commandline overrides to configuration options
let cmdConfig = parseCommandlineOptions();
database.updateCommandlineConfig(cmdConfig);
tally.updateCommandlineConfig(cmdConfig);


if(tally.config.frequency <= 0) { // on-demand sync
    await invokeImport();
    logger.closeStreams();
}
else { // continuous sync
    const triggerImport = async () => {
        try {
            // skip if sync is already running (wait for next trigger)
        if(!isSyncRunning) {
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
                //update local variable copy of last alter ID (only on success)
                lastMasterAlterId = masterAlterId;
                lastTransactionAlterId = transactionAlterId;
            }
        }
        } catch (err) {
            if(typeof err == 'string') { // e.g. company closed in Tally, next trigger will try again
                logger.logMessage(err + ' [%s]', new Date().toLocaleString());
            }
            else {
                throw err;
            }
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