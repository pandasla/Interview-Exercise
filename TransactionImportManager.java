
package oracle.apps.epm.arm.model.applicationModule.manager.importexport;

import com.hyperion.css.common.CSSUserIF;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.math.BigDecimal;

import java.net.MalformedURLException;
import java.net.URL;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import oracle.adf.model.adapter.dataformat.csv.CSVParser;

import oracle.apps.epm.fcm.common.model.applicationModule.manager.CommonManager;
import oracle.apps.epm.arm.model.applicationModule.manager.SOA.ARMHumanWorkflowManager;
import oracle.apps.epm.arm.model.applicationModule.manager.attribute.AttributeManager;
import oracle.apps.epm.arm.model.applicationModule.manager.currencyRates.CurrencyRatesManager;
import oracle.apps.epm.arm.model.applicationModule.manager.importexport.objectDefinitions.AttributeDef;
import oracle.apps.epm.arm.model.applicationModule.manager.importexport.objectDefinitions.CSVColumn;
import oracle.apps.epm.arm.model.applicationModule.manager.importexport.objectDefinitions.ImportTransactionsDef;
import oracle.apps.epm.arm.model.applicationModule.manager.importexport.objectDefinitions.ReferenceDef;
import oracle.apps.epm.arm.model.applicationModule.manager.query.FilterManager;
import oracle.apps.epm.arm.model.applicationModule.manager.reconciliationActions.AmortizationManager;
import oracle.apps.epm.arm.model.applicationModule.manager.reconciliationActions.ReconciliationAction;
import oracle.apps.epm.arm.model.applicationModule.manager.reconciliationActions.ReconciliationActionsManager;
import oracle.apps.epm.arm.model.applicationModule.manager.reconciliationActions.ReconciliationCurrencyBucket;
import oracle.apps.epm.arm.model.applicationModule.manager.reconciliationActions.TransactionCurrencyBucket;
import oracle.apps.epm.arm.model.applicationModule.manager.reconciliationActions.TransactionManager;
import oracle.apps.epm.arm.model.applicationModule.manager.reconciliationActions.Workflow;
import oracle.apps.epm.arm.model.applicationModule.manager.rules.ARMRuleEngine;
import oracle.apps.epm.arm.model.common.ARMmodelConstants;
import oracle.apps.epm.arm.model.common.Currency;
import oracle.apps.epm.arm.model.common.CustomDBTransactionImpl;
import oracle.apps.epm.arm.model.entity.TransactionEOImpl;
import oracle.apps.epm.arm.model.view.common.VOConstants;
import oracle.apps.epm.fcm.common.model.applicationModule.manager.query.QueryAttribute;
import oracle.apps.epm.fcm.common.model.applicationModule.manager.query.QueryConstants;
import oracle.apps.epm.fcm.common.model.applicationModule.manager.query.arm.ARMAttributeList;
import oracle.apps.epm.fcm.common.model.applicationModule.manager.query.arm.CurrencyBucket;
import oracle.apps.epm.fcm.common.model.applicationModule.manager.rules.RuleConstants;
import oracle.apps.epm.fcm.common.model.applicationModule.manager.rules.engine.RuleResult;
import oracle.apps.epm.fcm.common.model.applicationModule.manager.rules.engine.accessRule.AccessRuleResult;
import oracle.apps.epm.fcm.common.model.common.CalendarUtils;
import oracle.apps.epm.fcm.common.model.common.ImportError;
import oracle.apps.epm.fcm.common.model.common.ModelConstants;
import oracle.apps.epm.fcm.common.model.common.ModelLogger;
import oracle.apps.epm.fcm.common.model.common.ModelUtils;
import oracle.apps.epm.fcm.common.model.common.identityCache.Identity;
import oracle.apps.epm.fcm.common.model.common.identityCache.IdentityCache;

import oracle.core.ojdl.logging.ODLLevel;

import oracle.jbo.ApplicationModule;
import oracle.jbo.JboException;
import oracle.jbo.NameValuePairs;
import oracle.jbo.Row;
import oracle.jbo.RowSet;
import oracle.jbo.RowSetIterator;
import oracle.jbo.ViewObject;
import oracle.jbo.domain.generic.GenericBlob;
import oracle.jbo.server.ApplicationModuleImpl;
import oracle.jbo.server.DBTransaction;
import oracle.jbo.server.DBTransactionImpl;
import oracle.jbo.server.EntityImpl;
import oracle.jbo.server.ViewObjectImpl;
import oracle.jbo.server.ViewRowImpl;


public class TransactionImportManager extends ImportManager {

    private static HashMap<String, Integer> m_validColumns;
    private String[] m_data;
    private List<CurrencyBucket> m_currencyBuckets;
    private List<HashMap<String, Object>> m_formatTransAttributes;
    private HashSet<Long> m_validTransAttributes;
    private List<HashMap<String, Object>> m_formatActionPlanAttributes;
    private HashSet<Long> m_validActionPlanAttributes;

    private boolean m_bError;
    // For row number in pre-mapped import file (used for error message)
    private HashMap<Integer, Integer> rowNumberMap; 
    private int totalRowCount;

    public enum REFERENCES_TYPES {
        REFERENCES,
        ACTION_PLAN_REFERENCES;
    }

    public TransactionImportManager() {
        super();
    }
    
    
    public static TransactionImportManager getInstance()
    {
      TransactionImportManager inst = (TransactionImportManager)ModelUtils.getSettingObject("TRANSACTION_IMPORT_MANAGER");
      if (inst == null)
        {
          inst = new TransactionImportManager();
          ModelUtils.setSetting("TRANSACTION_IMPORT_MANAGER", inst);
        }
      return inst;
    }

    public static void cleanUp()
    {
      ModelUtils.removeSetting("TRANSACTION_IMPORT_MANAGER");
    }
    
    /**
    * Imports pre-mapped transactions from a binary file (uploaded file)
    *
    * @param p_am
    * @param p_importFile
    * @param p_lPeriodId
    * @param p_strTransactionType
    * @param p_iMode
    * @param p_dateFormat
    * @return
    */
    public HashMap importPreMappedTransactions(ApplicationModule p_am,
                                     GenericBlob p_importFile,
                                     Long p_lPeriodId,
                                     String p_strTransactionType,
                                     Integer p_iMode, ArrayList<String> p_dateFormat) {
       
        m_iMode = p_iMode;
        _initialize(p_am, TYPE_TRANSACTION, true);
        ARMRuleEngine.initialize();
        m_state.getConverter().setDateFormat(p_dateFormat);
        m_state.setPeriodId(p_lPeriodId);
        m_state.setTransactionType(p_strTransactionType);
        rowNumberMap = new HashMap<Integer, Integer>();
        totalRowCount = 0;
        
        // Read import file and store rows in ImportState
        try {
            
           p_am.getTransaction().rollback();
        
           BufferedReader br =
               new BufferedReader(new InputStreamReader(p_importFile.getInputStream(),
                                                        "UTF-8"));
           String strLine;
           String headers = null;
           HashMap<String, List<String>> groupRowsByAccount = new HashMap<String, List<String>>();
           HashMap<String, Integer> strLineToRowNumber = new HashMap<String, Integer>();
           int rowCount = 1;
            
           while ((strLine = br.readLine()) != null) {
               if (headers == null) {
                   headers = strLine;
                   // Parse headers. If required header column is missing then it should error out.
                   _parseHeaders(p_am, headers, true);
               } else {
                   // Fix for Bug 20786463 - DUPLICATE ACCOUNT ID ERROR OCCURS IF TRANSACTIONS NOT GROUPED BY ACCOUNT ID
                   // Group by reconciliation account ids before add to m_state
                   //m_state.addRow(strLine);
                   strLineToRowNumber.put(strLine, rowCount++);
                   InputStream is = new ByteArrayInputStream(strLine.getBytes("UTF-8"));
                   CSVParser parser = new CSVParser(is);
                   parser.nextLine();
                   String[] rowData = parser.getLineValues();
                   CSVColumn column = m_state.getColumn(RECONCILIATION_ACCOUNT_ID);
                   
                   if (rowData != null && column != null) {
                       String key = rowData[column.getColumn()];
                       if (groupRowsByAccount.containsKey(key)) {
                           List<String> value = groupRowsByAccount.get(key);
                           value.add(strLine);
                           groupRowsByAccount.put(key, value);
                       } else {
                           List<String> value = new LinkedList<String>();
                           value.add(strLine);
                           groupRowsByAccount.put(key, value);
                       }  
                   } else {
                       // Fix for Bug 20983439: when there is empty row then rowData became null and we got NPE
                       ModelLogger.logInfo("Empty row is read from Importing file at line " + (rowCount-1));
                   }
                }    
            }
        
            // Add rows sorted by account to m_state and also 
            // map reorders rows to file's original row numbers
            int count = 0;
            Set<String> keys = groupRowsByAccount.keySet();
            for (String key: keys) {
                List<String> rows = groupRowsByAccount.get(key);
                
                for (String row: rows) {
                    // Now add grouped by account rows to m_state
                    m_state.addRow(row);
                    
                    // Map reorders rows to file's original row numbers
                    int rowNumber = strLineToRowNumber.get(row);
                    rowNumberMap.put(count++, rowNumber);
                }
                
            }
        } catch (Exception e) {
           _logException(null, null, e);
        }

       
        Long lFormatId = null;
        Long lRateTypeId = null;
        Integer iStatusId= null;
        Long currentReconciliationId = null;
        String currentProfileId = null;
        String summaryRecon = null;
        ArrayList<Long> reconListForEmail = new ArrayList<Long>();
        
        Boolean bIsActionPlanDisplayed = false;
        TransactionCurrencyBucket transBucket = null;
        ReconciliationAction reconAction = null;
        boolean bFirstTransaction = true;
        boolean bHasPreventSaveRule = false;
        boolean bHasReqAttachmentRule = false;
        
        int iRow = -1;
        String newline = "\n";
        boolean bIsReconciliationNotExist = false;
        
        ArrayList<String> reconAccountIdList = new ArrayList<String>();
        ArrayList<String> reconAccountIdsAdded = new ArrayList<String>();
       
        // Get list of all reconciliation account ids first
        if (!m_errorHandler.hasErrors()) {
            
            for (int cnt=0; cnt<m_state.getNumRows(); cnt++) {
                try {
                   m_data = m_state.getRow(cnt);
                   iRow = rowNumberMap.get(cnt);
                } catch (Exception e) {
                   _logException(null, null, e);
                   break;
                }
                if (m_data != null) {
                   for (int i = 0; i < m_data.length; i++) {
                       if (m_data[i] != null) {
                           m_data[i] = m_data[i].replaceAll("\\\\n", newline);
                       }
                   }
                }
              
                
                if (m_data != null)
                {
                  String profileId = _getValue(RECONCILIATION_ACCOUNT_ID);
                  
                  // Bug fix for Bug 20807345
                  if (profileId == null) {
                      String msg = ModelUtils.getStringFromBundle("VALUE_IS_MISSING_MSG");
                      _logTranslatedError(iRow, 0, msg);
                      continue;
                  }
                 
                  // Check if new Reconciliation ID is found
                  if (currentProfileId == null || !currentProfileId.equalsIgnoreCase(profileId)) {
                   
                      // Check if already added reconciliation is coming back again
                      if (reconAccountIdList.contains(profileId.toUpperCase())) {
                          Map<String, Object> paramMap = new HashMap<String, Object>(1);
                          paramMap.put("ACCOUNT", profileId);
                          String msg = ModelUtils.getStringFromBundle("DUPLICATE_RECON_ACCOUNT_ID", paramMap);
                          _logTranslatedError(iRow, 0, msg);
                      } else {
                          reconAccountIdList.add(profileId.toUpperCase());
                      }
                      
                      currentProfileId = profileId;
                  }
              
                }
              else
                {
                  System.out.println(" EMPTY ROW " + cnt);
                }
              
            }
            
        }
        
        // For all the reconciliation account ids, get all the information needed for transaction importing
        // and store them in ImportState object.
        if (!m_errorHandler.hasErrors()) {
             m_state.setStatus(STATUS_IMPORTING);
            
            // Cache all reconciliations information in importTransactionsDefByReconId
            HashMap<String, ImportTransactionsDef> importTransactionsDefByReconId = new HashMap<String, ImportTransactionsDef>(100);
            // Splits reconciliation account ids in 1000 groups for IN cause query limitation.
            ArrayList<String> lCommaSeparatedReconAccountIdLists = ModelUtils.getSplitStringIdLists(reconAccountIdList);
            ViewObjectImpl preMappedReconsVO = null;
            
            // Count number of transactions deleted
            int deleteCount = 0;
            
            for (String strIdList : lCommaSeparatedReconAccountIdLists) {
                
                // Delete transactions and count the number
                m_state.setStatus(STATUS_DELETING);
                deleteCount = deleteCount + _deleteAllTransactions((ApplicationModuleImpl)p_am, p_lPeriodId, p_strTransactionType, strIdList);
                
                preMappedReconsVO = (ViewObjectImpl)p_am.findViewObject(VOConstants.VO_RECONCILIATION);
                preMappedReconsVO.setListenToEntityEvents(false);
                preMappedReconsVO.reset();
                preMappedReconsVO.setWhereClause(null);
                String strSubClause = "upper(ReconciliationEO.RECONCILIATION_ACCOUNT_ID) IN (" + strIdList + ")"; 
                preMappedReconsVO.setWhereClause(strSubClause + " AND ReconciliationEO.PERIOD_ID = " + p_lPeriodId);
                preMappedReconsVO.applyViewCriteria(null);
                preMappedReconsVO.executeQuery();
                      
                Long periodId = null;
                
                //
                // Store information for reconciliations existing for selected period 
                //
                while (preMappedReconsVO.hasNext()) {
                    Row row = preMappedReconsVO.next();
                    summaryRecon = (String)row.getAttribute(VOConstants.VO_RECONCILIATION_SUMMARY_RECONCILIATION);
                    currentReconciliationId = (Long)row.getAttribute(VOConstants.VO_RECONCILIATION_ID);
                    currentProfileId = (String)row.getAttribute(VOConstants.VO_RECONCILIATION_ACCOUNT_ID);
                    //periodId = (Long)row.getAttribute(VOConstants.VO_PERIOD_ID);
                    lFormatId = (Long)row.getAttribute(VOConstants.VO_FORMAT_ID);
                    lRateTypeId = (Long)row.getAttribute(VOConstants.VO_RATE_TYPE_ID);
                    iStatusId = (Integer)row.getAttribute(VOConstants.VO_STATUS_ID);
                    bIsReconciliationNotExist = false;
                    
                    reconAccountIdsAdded.add(currentProfileId.toUpperCase());
                    
                    if (!_validateUserSecurityScope(p_am, currentReconciliationId)) {
                        Map<String, Object> paramMap = new HashMap<String, Object>(1);
                        paramMap.put("ACCOUNT", currentProfileId);
                        String msg = ModelUtils.getStringFromBundle("NO_SECURITY_ACCESS_TO_RECON", paramMap);
                        _logTranslatedError(null, 0, msg);
                        break;
                    }
                    
                    if (summaryRecon.equalsIgnoreCase(ModelConstants.YES)) {
                        Map<String, Object> paramMap = new HashMap<String, Object>(1);
                        paramMap.put("ACCOUNT", currentProfileId);
                        String msg = ModelUtils.getStringFromBundle("SUMMARY_RECON_NOT_SUPPORTED", paramMap);
                        _logTranslatedError(null, 0, msg);
                        break;
                    }
                    
                    // If reconciliations status is closed or open with reviewer then open it
                    if (iStatusId == 1 || iStatusId == 10) {
                        row.setAttribute(VOConstants.VO_RECONCILIATION_STATUS_ID, new Integer(6));
                        row.setAttribute(VOConstants.VO_RECONCILIATION_RESPONSIBILITY_LEVEL, new Integer(0));
                        reconListForEmail.add(currentReconciliationId);
                        ModelLogger.logInfo("Reconciliations status changed to Open with Preparer: " + currentReconciliationId);
                    }
                    
                    // Get currency buckets, format attributes, and etc...
                    m_currencyBuckets = initBucketInfo(p_am, currentReconciliationId);
                    bIsActionPlanDisplayed = isActionPlanDisplayed(p_am, lFormatId, p_strTransactionType);

                    List<RuleConstants.RuleType> lRuleTypes = new LinkedList<RuleConstants.RuleType>();
                    lRuleTypes.add(RuleConstants.RuleType.RULE_TYPE_PREVENT_SAVE);
                    lRuleTypes.add(RuleConstants.RuleType.RULE_TYPE_ATTACHMENT_REQUIREMENT);

                    Long lRuleContext = m_state.getRuleContext();
                    lRuleContext = ARMRuleEngine.loadRulesForObject(lRuleContext, currentReconciliationId, "R", null, lRuleTypes, p_am);
                    m_state.setRuleContext(lRuleContext);


                    bHasPreventSaveRule = ARMRuleEngine.objectHasRulesOfType(lRuleContext, currentReconciliationId,
                                                           null,
                                                           p_strTransactionType,
                                                           RuleConstants.RuleType.RULE_TYPE_PREVENT_SAVE);
                    
                    bHasReqAttachmentRule = ARMRuleEngine.objectHasRulesOfType(lRuleContext, currentReconciliationId,
                                                           null,
                                                           p_strTransactionType,
                                                           RuleConstants.RuleType.RULE_TYPE_ATTACHMENT_REQUIREMENT);
                    _initFormatAttributes(p_am, currentReconciliationId, p_strTransactionType, false);
                    _initFormatAttributes(p_am, currentReconciliationId, _getActionPlanType(p_strTransactionType), true);
                    
                    // Check if m_formatTransAttributes is empty. 
                    // If it is then format doesn't support transaction type. Should error out.
                    if (m_formatTransAttributes.isEmpty()) {
                        Map<String, Object> paramMap = new HashMap<String, Object>(1);
                        paramMap.put("ACCOUNT", currentProfileId);
                        String msg = ModelUtils.getStringFromBundle("FORMAT_NOT_SUPPORT_TRANS_TYPE", paramMap);
                        _logTranslatedError(null, 0, msg);
                        break;
                    }
                    
                    // Use ImportTransactionsDef object to cache reconciliation related information for transaction importing in ImportState
                    ImportTransactionsDef transactionDef = new ImportTransactionsDef(currentReconciliationId,
                                                                                     m_iMode,
                                                                                     currentProfileId,
                                                                                     lRateTypeId,
                                                                                     transBucket,
                                                                                     reconAction,
                                                                                     bFirstTransaction,
                                                                                     bIsActionPlanDisplayed,
                                                                                     bHasPreventSaveRule,
                                                                                     bHasReqAttachmentRule,
                                                                                     bIsReconciliationNotExist,
                                                                                     m_currencyBuckets,
                                                                                     m_formatTransAttributes,
                                                                                     m_formatActionPlanAttributes);
                    
                    importTransactionsDefByReconId.put(currentProfileId.toUpperCase(), transactionDef);
                }
                
                //
                // Store information for reconciliations not existing for selected period
                //
                preMappedReconsVO = (ViewObjectImpl)p_am.findViewObject(VOConstants.VVO_PREMAPPED_TRANSACTION_RECONCILIATION);
                preMappedReconsVO.setListenToEntityEvents(false);
                preMappedReconsVO.reset();
                preMappedReconsVO.setWhereClause(null);
                strSubClause = "upper(RECONCILIATION_ACCOUNT_ID) IN (" + strIdList + ")"; 
                preMappedReconsVO.setWhereClause(strSubClause + " AND PERIOD_ID IS NULL");
                preMappedReconsVO.applyViewCriteria(null);
                preMappedReconsVO.executeQuery();
                
                while (preMappedReconsVO.hasNext()) {
                    Row row = preMappedReconsVO.next();
                    //periodId = (Long)row.getAttribute(VOConstants.VO_PERIOD_ID);
                    summaryRecon = (String)row.getAttribute(VOConstants.VO_RECONCILIATION_SUMMARY_RECONCILIATION);
                    currentReconciliationId = (Long)row.getAttribute(VOConstants.VO_RECONCILIATION_ID);
                    currentProfileId = (String)row.getAttribute(VOConstants.VO_RECONCILIATION_ACCOUNT_ID);
                    lFormatId = (Long)row.getAttribute(VOConstants.VO_FORMAT_ID);
                    lRateTypeId = (Long)row.getAttribute(VOConstants.VO_RATE_TYPE_ID);
                    iStatusId = (Integer)row.getAttribute(VOConstants.VO_STATUS_ID);
                    
                    if (!reconAccountIdsAdded.contains(currentProfileId.toUpperCase())) {
                        bIsReconciliationNotExist = true;
                        
                        if (!_validateUserSecurityScope(p_am, currentReconciliationId)) {
                            Map<String, Object> paramMap = new HashMap<String, Object>(1);
                            paramMap.put("ACCOUNT", currentProfileId);
                            String msg = ModelUtils.getStringFromBundle("NO_SECURITY_ACCESS_TO_RECON", paramMap);
                            _logTranslatedError(null, 0, msg);
                            break;
                        }
                        
                        if (summaryRecon.equalsIgnoreCase(ModelConstants.YES)) {
                            Map<String, Object> paramMap = new HashMap<String, Object>(1);
                            paramMap.put("ACCOUNT", currentProfileId);
                            String msg = ModelUtils.getStringFromBundle("SUMMARY_RECON_NOT_SUPPORTED", paramMap);
                            _logTranslatedError(null, 0, msg);
                            break;
                        }
                        
                        m_currencyBuckets = initBucketInfo(p_am, currentReconciliationId);
                        bIsActionPlanDisplayed = isActionPlanDisplayed(p_am, lFormatId, p_strTransactionType);
                        bHasPreventSaveRule = ARMRuleEngine.objectHasRulesOfType(m_state.getRuleContext(), 
                                                                                 currentReconciliationId,
                                                                                 null,
                                                                                 p_strTransactionType,
                                                                                 RuleConstants.RuleType.RULE_TYPE_PREVENT_SAVE);
                        
                        bHasReqAttachmentRule = ARMRuleEngine.objectHasRulesOfType(m_state.getRuleContext(), 
                                                                                   currentReconciliationId,
                                                                                   null,
                                                                                   p_strTransactionType,
                                                                                   RuleConstants.RuleType.RULE_TYPE_ATTACHMENT_REQUIREMENT);
                        _initFormatAttributes(p_am, currentReconciliationId, p_strTransactionType, false);
                        _initFormatAttributes(p_am, currentReconciliationId, _getActionPlanType(p_strTransactionType), true);
                        
                        // Check if m_formatTransAttributes is empty. 
                        // If it is then format doesn't support transaction type. Should error out.
                        if (m_formatTransAttributes.isEmpty()) {
                            Map<String, Object> paramMap = new HashMap<String, Object>(1);
                            paramMap.put("ACCOUNT", currentProfileId);
                            String msg = ModelUtils.getStringFromBundle("FORMAT_NOT_SUPPORT_TRANS_TYPE", paramMap);
                            _logTranslatedError(null, 0, msg);
                            break;
                        }
                        
                        // Use ImportTransactionsDef object to cache reconciliation related information for transaction importing in ImportState
                        ImportTransactionsDef transactionDef = new ImportTransactionsDef(currentReconciliationId,
                                                                                         m_iMode,
                                                                                         currentProfileId,
                                                                                         lRateTypeId,
                                                                                         transBucket,
                                                                                         reconAction,
                                                                                         bFirstTransaction,
                                                                                         bIsActionPlanDisplayed,
                                                                                         bHasPreventSaveRule,
                                                                                         bHasReqAttachmentRule,
                                                                                         bIsReconciliationNotExist,
                                                                                         m_currencyBuckets,
                                                                                         m_formatTransAttributes,
                                                                                         m_formatActionPlanAttributes);
                        
                        reconAccountIdsAdded.add(currentProfileId.toUpperCase());
                        importTransactionsDefByReconId.put(currentProfileId.toUpperCase(), transactionDef);
                    }
                    
                }
            }    
            
            // Cache all the information needed for transaction importing to ImportState object
            m_state.setDeleted(deleteCount);
            m_state.setImportTransactionsDefByReconId(importTransactionsDefByReconId);
            m_state.setReconciliationListForEmail(reconListForEmail);
            m_state.setIsForPremappedTransaction(true);
        }
        
        if (m_errorHandler.hasErrors()) {
            m_state.setStatus(STATUS_COMPLETE_EXCEPTIONS);
            m_state.setDeleted(0);
            List<ImportError> errors = m_errorHandler.getErrors();
            Collections.sort(errors);
            m_state.getResults().put("ERROR_LIST", errors);
            m_state.getResults().put("CREATED", 0);
            m_errorHandler.processErrors(p_am, m_state.getColumnHeaders());
            p_am.getTransaction().rollback();
        } else {
            m_state.setStatus(STATUS_IMPORTING);
        }
        
        ModelUtils.setSetting(ARM_IMPORT_STATE, m_state);
        
        // For testing without progress bar UI
        //continueImport(p_am);
        
        return m_state.getResults();
        
   }
   
    /**
     * Continues an import.  Assumes that importPreMappedTransactions
     * method has already been called.  Should be called continiously until
     * complete.  Between each call, the UI should update
     * 
     * @param p_am
     * @return HashMap
     */
    public HashMap continueImport(ApplicationModule p_am) {
        // Continuing a run from before. Get cached ImportState object.
        m_state = (ImportState)ModelUtils.getSettingObject(ARM_IMPORT_STATE);
        
        if (m_state == null) {
        
          m_state = new ImportState(TYPE_TRANSACTION, MODE_REPLACE_ALL);
          m_errorHandler = new ImportErrorManager();
          _logError(null, null, "INVALID_FILE_MSG");
          return m_state.getResults();
          
        } 
            
        Long currentReconciliationId = null;
        String currentReconAccountId = null;
        Long periodId= m_state.getPeriodId();
        
        m_iMode = m_state.getMode();
        m_errorHandler = m_state.getErrorHandler();
        m_state.getConverter().updateApplicationModule(p_am);
        Integer iStatus = (Integer)m_state.getResults().get("STATUS");          
        ModelUtils.setSetting(ARM_IMPORT_MODE, Boolean.TRUE);

        try {
            m_state.setStatus(STATUS_IMPORTING);
            
            Locale locale = ModelUtils.getLocale(p_am);
            String transactionType = m_state.getTransactionType();
            boolean isPremapped = m_state.getIsForPremappedTransaction();
            String lastReconAccountID = m_state.getLastReconAccountID();
            
            // Calculates block size to process importing at a time for progress bar
            int iCount = 1;
            int iRow = -1;
            int iTotal = m_state.getNumRows();
            int iBlock = iTotal/20;        
            iBlock = Math.max(iBlock, 100);                   
            iBlock = Math.min(iBlock, 500);
            
            
            while (m_state.hasNext() && iCount < iBlock) {
                // Get cached data for transaction importing
                m_data = m_state.getNextRow();
                
                // Fix for Bug 20860278 - ML TEXT IMPORTED VIA THE 'IMPORT PREMAPPED TRANS' DLG DOESN'T SEPARATE LINES
                if (m_data != null) {
                    for (int i = 0; i < m_data.length; i++) {
                        if (m_data[i] != null) {
                            m_data[i] = m_data[i].replaceAll("\\\\n", "\n");
                        }
                    }
                }
                
                // Get row number of importing file. Used for error message.
                iRow = rowNumberMap.get(totalRowCount++);
                
                String reconAccountId = _getValue(RECONCILIATION_ACCOUNT_ID);
                ImportTransactionsDef importTransactionsDef = m_state.getImportTransactionsDef(reconAccountId.toUpperCase());
                m_state.setLastReconAccountID(reconAccountId);
                
                // Check if reconciliation account ID is existing in ARM
                if (importTransactionsDef == null) {
                    Map<String, Object> paramMap = new HashMap<String, Object>(1);
                    paramMap.put("ACCOUNT", reconAccountId);
                    String msg = ModelUtils.getStringFromBundle("RECON_ACCOUNT_ID_NOT_FOUND", paramMap);
                    _logTranslatedError(iRow, 0, msg);
                    continue;
                }
                
                // If last transacction import is continued from same reconciliation account id
                if (currentReconAccountId == null && lastReconAccountID != null && 
                    lastReconAccountID.equalsIgnoreCase(reconAccountId)) {
                    // Load rules
                    String strUserRole = ModelUtils.getRoleCode(ModelUtils.getUserRole());
                 
                    currentReconAccountId = reconAccountId;
                    currentReconciliationId = importTransactionsDef.getReconciliationId();

                    Long lRuleContextId = m_state.getRuleContext();
                    lRuleContextId = ARMRuleEngine.loadRulesForObject(lRuleContextId, currentReconciliationId, "R", strUserRole, p_am);
                    m_state.setRuleContext(lRuleContextId);
                }
                
                // Check to see if new Reconciliation Account ID is found
                if (currentReconAccountId == null || !currentReconAccountId.equalsIgnoreCase(reconAccountId)) {
                                   
                    // Calculate previous transaction summary amount 
                    if (currentReconciliationId != null) {
                        TransactionManager.rebuildAllTransactionSummaries(p_am,
                                                                          currentReconciliationId,
                                                                          null,
                                                                          currentReconAccountId,
                                                                          periodId);
                        ReconciliationActionsManager.setPreparerUpdateDate(p_am,
                                                                           currentReconciliationId,
                                                                           false);
                    }
                    
                    // Reset currentReconAccountId and currentReconciliationId with new one
                    currentReconAccountId = reconAccountId;
                    currentReconciliationId = importTransactionsDef.getReconciliationId();

                    // Reload rules
                    String strUserRole = ModelUtils.getRoleCode(ModelUtils.getUserRole());
                    Long lRuleContextId = m_state.getRuleContext();                    
                    lRuleContextId = ARMRuleEngine.loadRulesForObject(lRuleContextId, currentReconciliationId, "R", strUserRole, p_am);
                    m_state.setRuleContext(lRuleContextId);
                    
                    // Need to clear period calendar cache so it will be reinitialized for new reconciliation
                    ModelUtils.removeSetting("PERIOD_CALENDAR_INFO_CACHE", false);
                    
                }
            
                // Process a transaction at a time
                _processTransaction(p_am, locale, periodId, transactionType, iRow, importTransactionsDef, isPremapped, m_state.getRuleContext());
                iCount++;
          } // block loop


          CustomDBTransactionImpl txn = (CustomDBTransactionImpl)p_am.getTransaction();
          txn.validate();
          txn.postChangesAndRemoveListeningEntities(true);
          
          // once more, this time clearing VO caches
          oracle.apps.epm.fcm.common.model.applicationModule.manager.CommonManager.freeApplicationModuleResources((ApplicationModuleImpl)p_am, true, true);

        }
        catch (Exception e) {
            _logException(null, null, e);
        }
  
  
        ModelUtils.setSetting(ARM_IMPORT_MODE, Boolean.FALSE);
        
        // we're all done with all blocks, time to wrap it up
        if (!m_state.hasNext()) {
            if (m_iMode != MODE_PASTE) {
                
              if (m_errorHandler.hasErrors()) {
              
                  m_state.setStatus(STATUS_COMPLETE_EXCEPTIONS);
                  m_state.setDeleted(0);
                  List<ImportError> errors = m_errorHandler.getErrors();
                  Collections.sort(errors);
                  m_state.getResults().put("ERROR_LIST", errors);
                  m_state.getResults().put("CREATED", 0);
                  m_errorHandler.processErrors(p_am, m_state.getColumnHeaders());
                  p_am.getTransaction().rollback();
                  
              } else {
                  
                  // Calculate last transaction summary amount before commit
                  if (currentReconciliationId != null) {
                      TransactionManager.rebuildAllTransactionSummaries(p_am,
                                                                        currentReconciliationId,
                                                                        null,
                                                                        currentReconAccountId,
                                                                        periodId);
                      ReconciliationActionsManager.setPreparerUpdateDate(p_am,
                                                                         currentReconciliationId,
                                                                         false);
                  }
                  
                  // Commit whole import transactions
                  p_am.getTransaction().commit();
                  
                  // Send email notifications for reopened reconciliations
                  ArrayList<Long> reconListForEmail = (ArrayList<Long>)m_state.getReconciliationListForEmail();
                  if (!reconListForEmail.isEmpty()) {
                      ModelLogger.logInfo("Sending email notifications to preparers");
                      ARMHumanWorkflowManager.notifyRecOpenWithPreparer((ApplicationModuleImpl)p_am, reconListForEmail);
                  }
              }
            }
            
            m_state.setComplete();
            ModelUtils.removeSetting(ARM_IMPORT_STATE);
        } /*else if (m_errorHandler.hasErrors()) {
            List<ImportError> errors = m_errorHandler.getErrors();
            Collections.sort(errors);
            m_state.getResults().put("ERROR_LIST", errors);
            m_errorHandler.processErrors(p_am, m_state.getColumnHeaders());
        }*/
        
        return m_state.getResults();
    }
    
    
    
    /**
     * Aborts and cleans up an import process
     * @return
     */
    public HashMap abortImport(ApplicationModule p_am) {
      m_state = (ImportState)ModelUtils.getSettingObject(ARM_IMPORT_STATE);
      if (m_state != null) {
        m_iMode = m_state.getMode();
        m_errorHandler = m_state.getErrorHandler();
        ModelUtils.removeSetting(ARM_IMPORT_STATE);
      } else {
        m_state = new ImportState(TYPE_TRANSACTION, MODE_REPLACE);
        m_errorHandler = new ImportErrorManager();
      }
      _logException(null, null, "ABORTED_BY_USER_MSG");
      m_state.setStatus(STATUS_ABORTED);
      m_errorHandler.processErrors(p_am, m_state.getColumnHeaders());
      p_am.getTransaction().rollback();
      return m_state.getResults();
    }
    
    /**
     * Returns the status of the current import
     * 
     * @return
     */
    public int getImportStatus() {
      m_state = (ImportState)ModelUtils.getSettingObject(ARM_IMPORT_STATE);
      if (m_state != null) {
        return (Integer)m_state.getResults().get("STATUS");
      } else {
        return STATUS_NOT_STARTED;
      }
    }


    /**
     * Imports transactions from a binary file (uploaded file)
     *
     * @param p_am
     * @param p_importFile
     * @param p_lReconciliationId
     * @param p_strTransactionType
     * @param p_iMode
     * @param p_dateFormat
     * @return
     */
    public HashMap importTransactions(ApplicationModule p_am,
                                      ReconciliationAction p_ReconAction,
                                      GenericBlob p_importFile,
                                      Long p_lReconciliationId,
                                      String p_strTransactionType,
                                      Integer p_iMode, ArrayList<String> p_dateFormat) {
        m_iMode = p_iMode;
        _initialize(p_am, TYPE_TRANSACTION, true);
        m_state.getConverter().setDateFormat(p_dateFormat);

        Locale locale = ModelUtils.getLocale(p_am);
        m_currencyBuckets = initBucketInfo(p_am, p_lReconciliationId);

        try {
            p_am.getTransaction().rollback();

            BufferedReader br =
                new BufferedReader(new InputStreamReader(p_importFile.getInputStream(),
                                                         "UTF-8"));
            String strLine;
            String headers = null;
            while ((strLine = br.readLine()) != null) {
                if (headers == null) {
                    headers = strLine;
                } else {
                    m_state.addRow(strLine);
                }
            }

            _parseHeaders(p_am, headers);

        } catch (Exception e) {
            _logException(null, null, e);
        }

        ViewObject reconVo = CommonManager.executeQueryWithCriteria(p_am, 
                                                                    VOConstants.VVO_RECONCILIATION, 
                                                                    VOConstants.VVO_RECONCILIATIONS_BY_ID_CRITERIA,
                                                                    VOConstants.VO_BIND_RECONCILIATION_ID,
                                                                    p_lReconciliationId);

        RowSetIterator rsi = reconVo.createRowSetIterator(null);
        Long lFormatId = null;
        Long lRateTypeId = null;
        Long lPeriodId = null;
        String sReconAccountId = null;
        if (rsi.hasNext()) {
            Row row = rsi.next();
            lFormatId = (Long)row.getAttribute(VOConstants.VO_FORMAT_ID);
            lRateTypeId = (Long)row.getAttribute(VOConstants.VO_RATE_TYPE_ID);
            lPeriodId = (Long)row.getAttribute(VOConstants.VO_PERIOD_ID);
            sReconAccountId = (String)row.getAttribute(VOConstants.VO_RECONCILIATION_ACCOUNT_ID);
        } else {
            _logException(null, null,
                          "Could not find reconciliation " + p_lReconciliationId);
        }
        rsi.closeRowSetIterator();

        ViewObject transVo = p_am.findViewObject(VOConstants.VO_TRANSACTIONS);
        Boolean bIsActionPlanDisplayed =
            isActionPlanDisplayed(p_am, lFormatId, p_strTransactionType);


        if (!m_errorHandler.hasErrors() && m_iMode == MODE_REPLACE_ALL) {
            m_state.setStatus(STATUS_DELETING);
            try {
                transVo.applyViewCriteria(transVo.getViewCriteriaManager().getViewCriteria(VOConstants.VO_TRANSACTIONS_BY_RECON_ID_AND_TYPE_CRITERIA));
                transVo.setNamedWhereClauseParam(VOConstants.VO_BIND_RECONCILIATION_ID,
                                                 p_lReconciliationId);
                transVo.setNamedWhereClauseParam(VOConstants.VO_BIND_TRANSACTION_TYPE,
                                                 p_strTransactionType);
                transVo.executeQuery();
                rsi = transVo.createRowSetIterator(null);
                RuleResult ruleResult = null;
                String fromFlag = null;
                Long lTransactionId = null;
                String strTransactionCode = null;
                String strMessage = null;
                int iCount = 0;
                while (rsi.hasNext()) {
                    Row transRow = rsi.next();
                    
                    // Skip pre-mapped transaction
                    fromFlag = (String)transRow.getAttribute(VOConstants.VO_TRANSACTIONS_FROM_FLAG);
                    if (fromFlag.equals("P")) {
                      continue;
                    }
                  
                    // if any of the existing transactions could not be deleted due to rules
                    // set import to error
                    lTransactionId = (Long)transRow.getAttribute(VOConstants.VO_TRANSACTIONS_ID);
                    strTransactionCode = (String)transRow.getAttribute(VOConstants.VO_TRANSACTION_DETAIL_CODE);
                    ruleResult =
                            ARMRuleEngine.isTransactionDeletable(p_ReconAction.getRulesContextId(),
                                                                 p_lReconciliationId,
                                                                 lTransactionId,
                                                                 p_strTransactionType,
                                                                 p_am);
                    if (ruleResult != null && ruleResult.isPositive()) {
                        // prevent deletion rules are positive
                        Map<String, Object> paramMap =
                            new HashMap<String, Object>(2);
                        if (strTransactionCode==null){
                            strTransactionCode = "";
                        }
                        paramMap.put("CODE", strTransactionCode);
                        paramMap.put("RULE", ruleResult.getMessage());
                        strMessage =
                                ModelUtils.getStringFromBundle("DELETE_TRANS_RULE_ERROR_MSG",
                                                               paramMap);
                        _logTranslatedError(null, null, strMessage);
                        m_bError = true;
                        continue;
                    }
                    transRow.remove();
                    iCount++;
                }
                rsi.closeRowSetIterator();
                if (m_bError){
                    m_state.setStatus(STATUS_COMPLETE_ERRORS);
                } 
                m_state.setDeleted(iCount);
            } catch (Exception e) {
                _logException(null, null, e);
            }
        }

        if (!m_errorHandler.hasErrors()) {
            m_state.setStatus(STATUS_IMPORTING);
            
            // check if there is any prevent transaction save rules for the transaction type
            boolean bHasPreventSaveRule =
                ARMRuleEngine.objectHasRulesOfType(p_ReconAction.getRulesContextId(), p_lReconciliationId,
                                                   null,
                                                   p_strTransactionType,
                                                   RuleConstants.RuleType.RULE_TYPE_PREVENT_SAVE);

            boolean bHasReqAttachmentRule =
                ARMRuleEngine.objectHasRulesOfType(p_ReconAction.getRulesContextId(), p_lReconciliationId,
                                                   null,
                                                   p_strTransactionType,
                                                   RuleConstants.RuleType.RULE_TYPE_ATTACHMENT_REQUIREMENT);

            _initFormatAttributes(p_am, p_lReconciliationId,
                                  p_strTransactionType, false);
            _initFormatAttributes(p_am, p_lReconciliationId,
                                  _getActionPlanType(p_strTransactionType),
                                  true);

            int iRow = 1;
            String newline = "\n";
            TransactionCurrencyBucket transBucket = null;
            ReconciliationAction reconAction = null;
            boolean bFirstTransaction = true;
            
            ImportTransactionsDef importTransactionsDef = new ImportTransactionsDef(p_lReconciliationId,
                                                                             m_iMode,
                                                                             sReconAccountId,
                                                                             lRateTypeId,
                                                                             transBucket,
                                                                             reconAction,
                                                                             bFirstTransaction,
                                                                             bIsActionPlanDisplayed,
                                                                             bHasPreventSaveRule,
                                                                             bHasReqAttachmentRule,
                                                                             false,
                                                                             m_currencyBuckets,
                                                                             m_formatTransAttributes,
                                                                             m_formatActionPlanAttributes);

            while (m_state.hasNext()) {
                m_bError = false;
                
                try {
                    m_data = m_state.getNextRow();
                    if (m_data != null) {
                        for (int i = 0; i < m_data.length; i++) {
                            if (m_data[i] != null) {
                                m_data[i] =
                                        m_data[i].replaceAll("\\\\n", newline);
                            }
                        }
                    }
                    
                    // Process a transaction at a time
                    _processTransaction(p_am, locale, lPeriodId, p_strTransactionType, iRow, importTransactionsDef, false, p_ReconAction.getRulesContextId());
               
                } catch (Exception e) {
                    _logException(null, null, e);
                }

                iRow++;
            }
        }

        if (m_errorHandler.hasErrors()) {
            m_state.setStatus(STATUS_COMPLETE_ERRORS);
            m_state.setDeleted(0);                                                    
            m_errorHandler.processErrors(p_am, m_state.getColumnHeaders());
            p_am.getTransaction().rollback();
            HashMap<String, Object> results = m_state.getResults();
            results.put("CREATED", 0);
            return results;
        } else {
            m_state.setStatus(STATUS_COMPLETE_SUCCESS);
            TransactionManager.rebuildAllTransactionSummaries(p_am,
                                                              p_lReconciliationId,
                                                              null,
                                                              sReconAccountId,
                                                              lPeriodId);
            ReconciliationActionsManager.setPreparerUpdateDate(p_am,
                                                               p_lReconciliationId,
                                                               false);
            p_am.getTransaction().commit();
            return m_state.getResults();
        }
    }
    
    /**
     * This method imports one transaction for given input parameters such as ImportTransactionsDef.
     * This is a common method used for Premapped and regular transaction importing.
     */
    private void _processTransaction(ApplicationModule p_am,
                                     Locale p_locale,
                                     Long p_lPeriodId, 
                                     String p_strTransactionType,
                                     int iRow,
                                     ImportTransactionsDef importTransactionsDef,
                                     boolean p_isPremapped,
                                     Long p_lRuleContext) {
        List<Row> rows = new LinkedList<Row>();
        //TODO: Is p_am.findViewObject() expensive call? If so then I have to remove it and get it as input parameters
        ViewObject transVo = p_am.findViewObject(VOConstants.VO_TRANSACTIONS);
        ViewObject amountVo = p_am.findViewObject(VOConstants.VO_TRANSACTION_AMOUNT);
        ViewObject amortVo = p_am.findViewObject(VOConstants.VO_AMORTIZATION);
        ViewObject transActionPlanVo = p_am.findViewObject(VOConstants.VO_TRANSACTION_ACTION_PLAN_DUPLICATE);
        
        ReconciliationAction reconAction = importTransactionsDef.getReconciliationAction();
        TransactionCurrencyBucket transBucket = importTransactionsDef.getTransactionBucket();
        List<CurrencyBucket> currencyBuckets = importTransactionsDef.getCurrencyBuckets();
        String strReconAccountId = importTransactionsDef.getReconciliationAccountId();
        Long lReconciliationId = importTransactionsDef.getReconciliationId();
        Long lRateTypeId = importTransactionsDef.getRateTypeId();
        boolean isFirstTransaction = importTransactionsDef.getIsFirstTransaction();
        boolean isAmortizationInfoInitialized = importTransactionsDef.getIsAmortizationInfoInitialized();
        boolean hasPreventSaveRule = importTransactionsDef.getHasPreventSaveRule();  
        boolean hasReqAttachmentRule = importTransactionsDef.getHasReqAttachmentRule(); 
        boolean isActionPlanDisplayed = importTransactionsDef.getIsActionPlanDisplayed();
        boolean isReconNotExist = importTransactionsDef.getIsReconciliationNotExist();
        //TODO: for summary reconciliation, summary created to be set to "Y"
        //String strSummaryCreated = importTransactionsDef.getSummaryReconciliationCreated();
        String strSummaryCreated = "N";

        Object strCode = _processStandardAttribute(QueryConstants.QA_ARM_TRANSACTION_CODE, false,
                                 null /* not passing row, batch mode*/, VOConstants.VO_TRANSACTION_DETAIL_CODE,
                                 iRow, TRANSACTION_CODE, 50, QueryAttribute.ATTR_TYPE_TEXT, true);
        Object strDesc = _processStandardAttribute(QueryConstants.QA_ARM_TRANSACTION_DESCRIPTION, false,
                                 null /* not passing row, batch mode*/, VOConstants.VO_TRANSACTION_DETAIL_DESCRIPTION,
                                 iRow, TRANSACTION_DESCRIPTION, 2000, QueryAttribute.ATTR_TYPE_TEXT, true);
        Object dOpenDate = _processStandardAttribute(QueryConstants.QA_ARM_TRANSACTION_OPEN_DATE, false, 
                                  null /* not passing row, batch mode*/, VOConstants.VO_TRANSACTION_DETAIL_OPEN_DATE,
                                  iRow, OPEN_DATE, null, QueryAttribute.ATTR_TYPE_DATE, true);
        Object dCloseDate = _processStandardAttribute(QueryConstants.QA_ARM_TRANSACTION_CLOSE_DATE, false,
                                  null /* not passing row, batch mode*/, VOConstants.VO_TRANSACTION_DETAIL_CLOSE_DATE,
                                  iRow, CLOSE_DATE, null, QueryAttribute.ATTR_TYPE_DATE, true);
  

        
        try {  
            Row rowTransaction = null;
            if (p_isPremapped && isReconNotExist) {
                rowTransaction = transVo.createAndInitRow(new NameValuePairs(new String[] { VOConstants.VO_RECONCILIATION_ID,
                                                                            VOConstants.VO_TRANSACTIONS_TRANSACTION_TYPE,
                                                                            VOConstants.VO_TRANSACTIONS_SUMMARY_CREATED,
                                                                            VOConstants.VO_TRANSACTIONS_AGE,
                                                                            VOConstants.VO_TRANSACTIONS_UPDATED,
                                                                            VOConstants.VO_TRANSACTIONS_FROM_FLAG,
                                                                            VOConstants.VO_TRANSACTIONS_PERIOD_ID,
                                                                            VOConstants.VO_TRANSACTIONS_RECON_ACCOUNT_ID,
                                                                            VOConstants.VO_TRANSACTIONS_STATUS_FLAG,
                                                                            VOConstants.VO_TRANSACTIONS_AMORT_ACCRETE,
                                                                            VOConstants.VO_TRANSACTION_DETAIL_CODE,
                                                                            VOConstants.VO_TRANSACTION_DETAIL_DESCRIPTION,
                                                                            VOConstants.VO_TRANSACTION_DETAIL_OPEN_DATE,
                                                                            VOConstants.VO_TRANSACTION_DETAIL_CLOSE_DATE
                                                                            },
                                                            new Object[] { new Long(-1),
                                                                            p_strTransactionType,
                                                                            strSummaryCreated,
                                                                            null,
                                                                            strSummaryCreated,
                                                                            "P",
                                                                            p_lPeriodId,
                                                                            strReconAccountId,
                                                                            ModelConstants.NO,
                                                                            ModelConstants.NO,
                                                                            strCode,
                                                                            strDesc,
                                                                            dOpenDate,
                                                                            dCloseDate                                                                   
                                                                           }));
            } else {
                String fromFlag = "I";
                if (p_isPremapped) {
                    fromFlag = "P";
                }
                rowTransaction = transVo.createAndInitRow(new NameValuePairs(new String[] {
                                                                      VOConstants.VO_RECONCILIATION_ID,
                                                                      VOConstants.VO_TRANSACTIONS_TRANSACTION_TYPE,
                                                                      VOConstants.VO_TRANSACTIONS_SUMMARY_CREATED,
                                                                      VOConstants.VO_TRANSACTIONS_AGE,
                                                                      VOConstants.VO_TRANSACTIONS_UPDATED,
                                                                      VOConstants.VO_TRANSACTIONS_FROM_FLAG,
                                                                      VOConstants.VO_TRANSACTIONS_PERIOD_ID,
                                                                      VOConstants.VO_TRANSACTIONS_RECON_ACCOUNT_ID,
                                                                      VOConstants.VO_TRANSACTIONS_STATUS_FLAG,
                                                                      VOConstants.VO_TRANSACTIONS_AMORT_ACCRETE,
                                                                      VOConstants.VO_TRANSACTION_DETAIL_CODE,
                                                                      VOConstants.VO_TRANSACTION_DETAIL_DESCRIPTION,
                                                                      VOConstants.VO_TRANSACTION_DETAIL_OPEN_DATE,
                                                                      VOConstants.VO_TRANSACTION_DETAIL_CLOSE_DATE
                                                                      },
                                                            new Object[] { 
                                                                      lReconciliationId,
                                                                      p_strTransactionType,
                                                                      strSummaryCreated,
                                                                      null,
                                                                      strSummaryCreated,
                                                                      fromFlag,
                                                                      p_lPeriodId,
                                                                      strReconAccountId,
                                                                      ModelConstants.NO,
                                                                      ModelConstants.NO, // assume no, override below
                                                                      strCode,
                                                                      strDesc,
                                                                      dOpenDate,
                                                                      dCloseDate                                                                                                                                                                                                             
                                                                      }));
            }
            rows.add(rowTransaction);
            Long id = (Long)rowTransaction.getAttribute(VOConstants.VO_TRANSACTION_ID);
              
              
          // recalculate attribute role access using rules with 1st transaction and cache it
          if (isFirstTransaction) {
              // post 1st transaction row to db to execute set attribute access rules
              p_am.getTransaction().postChanges();
              
              // execute rules for transaction and action plan attributes  
              _executeSetAttributeAccessRules(p_am,
                                              p_lRuleContext,
                                              lReconciliationId,
                                              id,
                                              p_strTransactionType,
                                              false);
              _executeSetAttributeAccessRules(p_am,
                                              p_lRuleContext,
                                              lReconciliationId,
                                              id,
                                              _getActionPlanType(p_strTransactionType),
                                              true);
    
              importTransactionsDef.setIsFirstTransaction(false); // Must validate if this flag is stored in m_state.
          }
    

          /*                    String strFromFDMEE = _processYesNo(row,
                        VOConstants.VO_TRANSACTION_FROM_FDMEE,
                        iRow, FROM_FDMEE, true);
          boolean bIsFromFDMEE = "Y".equals(strFromFDMEE); */
          String strAmortizing = _processAmortization(rowTransaction, VOConstants.VO_TRANSACTIONS_AMORT_ACCRETE, iRow, AMORT_ACCRETE, true);
    
          // handle amortized transactions
          boolean bAmortizing = "A".equals(strAmortizing);
          boolean bAccreting = "C".equals(strAmortizing);
          Row amortRow = null;
          if (!isReconNotExist && (bAmortizing || bAccreting)) {
              amortRow = _generateAmortization(amortVo, id, iRow, p_locale);
          } else if (isReconNotExist && (bAmortizing || bAccreting)) {
              // Amortization is not supported if reconciliation is not existing
              _error(AMORT_ACCRETE, iRow, "AMORTIZING_NOT_SUPPORTED_MSG");
          }
    
          _generateAttributeValues(p_am,
                                   p_am.findViewObject(VOConstants.VO_ATTRIBUTE_VALUE_ASSIGNMENTSVO),
                                   rows, id, iRow, false, p_isPremapped);
          Integer nAttachmentCount =
              _generateReferences(p_am.findViewObject(VOConstants.VO_REFERENCES),
                                  rows, id, iRow,
                                  REFERENCES_TYPES.REFERENCES);
    
          int bucketIndex = 1;
          int iterateIndex = 1;  
          Iterator<CurrencyBucket> bucketIter = currencyBuckets.iterator();
          BigDecimal baseValue = null;
          BigDecimal valueToWrite = null;
          String baseCurrency = null;
          String currencyToWrite = null;
          while (bucketIter.hasNext()) {
              CurrencyBucket bucket = bucketIter.next();
              
              // Fix for Bug 21036722
              // If it is for pre-mapped transaction then change bucketIndex to absolute index (not positional index)
              if (p_isPremapped) {
                int buckectId = bucket.getBucketId().intValue();
                // Get bucketIndex from bucket_id; e.g. 10001(Entered) = 1, 10002(Functional) = 2, 10003(Reporting) = 3
                bucketIndex = (buckectId % 10000); 
                ModelLogger.logFine("bucketIndex is " + bucketIndex);
              }
              
              Row amountRow =
                  amountVo.createAndInitRow(new NameValuePairs(new String[] { VOConstants.VO_OBJECT_ID,
                                                                              VOConstants.VO_CURRENCY_BUCKET_ID },
                                                               new Object[] { id,
                                                                              bucket.getBucketId() }));   
              valueToWrite = new BigDecimal(0);
              currencyToWrite =
                      _getValue(AMOUNT_CURRENCY + bucketIndex);
              if (currencyToWrite == null) {
                  currencyToWrite = bucket.getDefaultCurrency();
              } else if (!"SYS".equals(currencyToWrite)) {
                  if (!m_state.getConverter().isCurrencyValid(currencyToWrite)) {
                      _error(AMOUNT_CURRENCY + bucketIndex, iRow,
                             "INVALID_CURRENCY_MSG");
                  }
              }
              BigDecimal estimatedAmount = null;
              if (baseValue != null && baseCurrency != null) {
                  estimatedAmount =
                          CurrencyRatesManager.getConvertedCurrencyValue(p_am,
                                                                         lRateTypeId,
                                                                         p_lPeriodId,
                                                                         baseCurrency,
                                                                         currencyToWrite,
                                                                         baseValue);
              }
    
              // for a non-amortizing transaction, use the value from the file
              if (!bAmortizing && !bAccreting) {
                  String amount = _getValue(AMOUNT + bucketIndex);
                  if (amount == null) {
                      // handle case of FDMEE transaction that is incompletely
                      //  filled out (legitimate case).  Don't write out any
                      //  amount information
          /*                                if (bIsFromFDMEE) {
                          amountRow.setAttribute(VOConstants.VO_TRANSACTIONS_SKIP_INSERT, "Y");
                      } */
                      if (estimatedAmount != null) {
                          valueToWrite = estimatedAmount;
                      }
                  } else if (amount != null) {
                      BigDecimal newValue = null;
                      try {
                          newValue =
                                  m_state.getConverter().convertFormattedFloat(amount,
                                                                               p_locale);
                      } catch (ImportException e) {
                          _error(AMOUNT + bucketIndex, iRow,
                                 e.getMessage());
                      }
                      if (newValue != null) {
                          valueToWrite = newValue;
                          if (iterateIndex != 1 &&
                              newValue.compareTo(estimatedAmount) != 0) {
                              ModelLogger.logFine("Amount is override. iterateIndex is " + iterateIndex);
                              _writeData(amountRow,
                                         VOConstants.VVO_TRANSACTION_AMOUNTS_OVERRIDE,
                                         ModelConstants.YES);
                          }
                      }
                  }
              } else { // for amortizing transactions, calculate the value
                  // if there's already an error, don't bother calculating anything
                  if (!m_bError) {
                      if (!isAmortizationInfoInitialized) {
                          Row reconRow;
                          if (!p_isPremapped) {
                              reconRow = p_am.findViewObject(VOConstants.VVO_RECONCILIATION_ACTIONS).first();
                          } else {
                              ViewObject vo = CommonManager.executeQueryWithVariable(p_am, VOConstants.VVO_RECONCILIATION_ACTIONS,
                                                                                                  VOConstants.VO_BIND_RECONCILIATION_ID,
                                                                                                  lReconciliationId);
                              reconRow = vo.first();
                          }
                          
                          LinkedList<Workflow> workflow =
                              new LinkedList<Workflow>();
                          reconAction =
                                  new ReconciliationAction(p_am, lReconciliationId,
                                                           reconRow,
                                                           workflow);
                          HashMap<Long, ReconciliationCurrencyBucket> reconciliationCurrencyBuckets =
                              ReconciliationActionsManager.initializeReconciliationCurrencyBuckets(p_am, reconAction);
                          reconAction.setReconciliationBuckets(reconciliationCurrencyBuckets);
          
                          reconAction.setTransactionCurrencyBuckets(ReconciliationActionsManager.initTransactionCurrencyBuckets(reconAction));
                          transBucket = new TransactionCurrencyBucket(bucket);
                          
                          importTransactionsDef.setIsAmortizationInfoInitialized(true);
                          importTransactionsDef.setReconciliationAction(reconAction);
                          importTransactionsDef.setTransactionBucket(transBucket);
                      }
                      // if baseValue has been assigned, the the current amount
                      //  needs to be translated based on the base bucket
                      if (null != baseValue) {
                          valueToWrite =
                                  CurrencyRatesManager.getConvertedCurrencyValue(p_am,
                                                                                 lRateTypeId,
                                                                                 p_lPeriodId,
                                                                                 baseCurrency,
                                                                                 currencyToWrite,
                                                                                 baseValue);
          
                      } else {
                          if (bAccreting) {
                              ArrayList<BigDecimal> vals =
                                  AmortizationManager.getCumulativeAmortizedAmounts(p_am, 
                                                                                    id,
                                                                                    p_lPeriodId, 
                                                                                    reconAction.getCalendarId(), 
                                                                                    lRateTypeId,
                                                                                    reconAction.isHistoricalRateEnabled(),
                                                                                    transBucket.getCurrencyBucket().getBucketId(),
                                                                                    transBucket.getCurrency(),
                                                                                    transBucket.getCurrency(),
                                                                                    amortRow,
                                                                                    bAccreting);
                              valueToWrite = vals.get(0);
                          }
                          else {
                              valueToWrite =
                                    AmortizationManager.getEndingAmount(p_am,
                                                                        id,
                                                                        transBucket.getCurrency(),
                                                                        transBucket.getCurrency(),
                                                                        reconAction.getPeriodId(),
                                                                        reconAction.getCalendarId(),
                                                                        transBucket.getCurrencyBucket().getBucketId(),
                                                                        reconAction.getRateTypeId(),
                                                                        reconAction.isHistoricalRateEnabled(),
                                                                        bAccreting);
                          }
                      }
                  }
              }
              // "convert" the imported value to be rounded to the
              //  number of digits as defined by the currency (bug 16046069)
              Integer iDigits =
                  Currency.getCurrencyDecimalPlaces(currencyToWrite);
              valueToWrite =
                      valueToWrite.setScale(iDigits == null ? 0 :
                                            iDigits.intValue(),
                                            ARMmodelConstants.ARM_ROUNDING_MODE);
    
              _writeData(amountRow,
                         VOConstants.VO_TRANSACTION_AMOUNT_AMOUNT,
                         valueToWrite);
              _writeData(amountRow,
                         VOConstants.VO_TRANSACTION_AMOUNT_CURRENCY,
                         currencyToWrite);
              if (baseValue == null && baseCurrency == null) {
                  baseValue = valueToWrite;
                  baseCurrency = currencyToWrite;
              }
              bucketIndex++;
              iterateIndex++;
            }
    
            // Generate Action Plan
            if (isActionPlanDisplayed) {
                _generateActionPlan(p_am, transActionPlanVo, id, iRow, rows, p_isPremapped);
            }
          
              // Checks Prevent Save and Required Attachment rules 
            _checkPreventSaveAndReqAttachmentRules(p_am,
                                                    hasPreventSaveRule, hasReqAttachmentRule,
                                                    lReconciliationId, id, p_strTransactionType, 
                                                    nAttachmentCount, iRow,
                                                    p_lRuleContext);
            
            // Fix for Bug 20871302 - IMPORTED TRANSACTIONS DON'T DISPLAY AGE AND AGING VIOLATION VALUES
            TransactionEOImpl transactionEO = (TransactionEOImpl)((ViewRowImpl)rowTransaction).getEntityForAttribute("TransactionId");
            TransactionManager.updateAging(p_am, transactionEO);
            
            if (!m_bError) {
                m_state.flagSuccess(id, false);
            }
            
        } catch (Exception e) {
            _logException(iRow, null, e);
            //Iterator<Row> iter = rows.iterator();
            //while (iter.hasNext()) {
            //row.remove(); //TODO: are we allowing partial updates?
            //}
        }
        
    }


    /**
     * Initializes the bucket metadata
     * @param p_am
     * @param p_lReconciliationId
     */
    static List<CurrencyBucket> initBucketInfo(ApplicationModule p_am,
                                               Long p_lReconciliationId) {
        List<CurrencyBucket> currencyBuckets =
            new LinkedList<CurrencyBucket>();
        HashMap<String, Object> hBindVariablesAndValues =
            new HashMap<String, Object>(1);
        hBindVariablesAndValues.put(VOConstants.VO_BIND_RECONCILIATION_ID,
                                    p_lReconciliationId);

        ViewObject reconBucketsVVO =
            CommonManager.executeQueryWithCriteria(p_am,
                                                   VOConstants.VVO_RECONCILIATION_BUCKETS,
                                                   VOConstants.VVO_CURRENCY_BUCKETS_ENABLED_BY_RECONCILIATION_ID_CRITERIA,
                                                   hBindVariablesAndValues);

        CurrencyBucket currencyBucket = null;
        Long lBucketId = null;
        String strBucketName = null;
        Integer iBucketOrder = null;
        String strDefaultCurrency = null;
        reconBucketsVVO.executeQuery();
        RowSetIterator rsi = reconBucketsVVO.createRowSetIterator(null);
        if (rsi.hasNext()) {
            while (rsi.hasNext()) {
                Row bucketRow = rsi.next();

                lBucketId =
                        (Long)bucketRow.getAttribute(VOConstants.VO_CURRENCY_BUCKETS_BUCKET_ID);
                strBucketName =
                        (String)bucketRow.getAttribute(VOConstants.VO_CURRENCY_BUCKETS_BUCKET_LABEL);
                iBucketOrder =
                        (Integer)bucketRow.getAttribute(VOConstants.VO_CURRENCY_BUCKETS_BUCKET_ORDER);
                strDefaultCurrency =
                        (String)bucketRow.getAttribute(VOConstants.VO_CURRENCY_BUCKETS_RECON_DEFAULT_CURRENCY_ID);
                if (strDefaultCurrency == null ||
                    "SYS".equals(strDefaultCurrency)) {
                    strDefaultCurrency =
                            (String)bucketRow.getAttribute(VOConstants.VO_CURRENCY_BUCKETS_DEFAULT_CURRENCY_ID);
                }
                currencyBucket =
                        new CurrencyBucket(lBucketId, iBucketOrder, strBucketName,
                                           strDefaultCurrency, true);
                currencyBuckets.add(currencyBucket);
            }
        } else {
            ViewObject defaultBucketVVO =
                CommonManager.executeQueryWithCriteria(p_am,
                                                       VOConstants.VVO_CURRENCY_BUCKETS,
                                                       VOConstants.VO_SINGLE_CURRENCY_BUCKET_VO_CRITERIA);

            Row bucketRow = defaultBucketVVO.first();
            lBucketId =
                    (Long)bucketRow.getAttribute(VOConstants.VO_CURRENCY_BUCKETS_BUCKET_ID);
            strBucketName =
                    (String)bucketRow.getAttribute(VOConstants.VO_CURRENCY_BUCKETS_BUCKET_LABEL);
            iBucketOrder =
                    (Integer)bucketRow.getAttribute(VOConstants.VO_CURRENCY_BUCKETS_BUCKET_ORDER);
            strDefaultCurrency =
                    (String)bucketRow.getAttribute(VOConstants.VO_CURRENCY_BUCKETS_DEFAULT_CURRENCY_ID);
            currencyBucket =
                    new CurrencyBucket(lBucketId, iBucketOrder, strBucketName,
                                       strDefaultCurrency, true);
            currencyBuckets.add(currencyBucket);
        }
        rsi.closeRowSetIterator();
        return currencyBuckets;
    }

    /**
     * Parses the headers (first row) and populates the m_headers object.
     * By default, it passes p_preMapped=false.
     *
     * @param p_headers
     * @return
     */
    private void _parseHeaders(ApplicationModule p_am, String p_headers) {
        _parseHeaders(p_am, p_headers, false);
    }

    /**
     * Parses the headers (first row) and populates the m_headers object
     *
     * @param p_headers
     * @param p_preMapped
     * @return
     */
    private void _parseHeaders(ApplicationModule p_am, String p_headers, boolean p_preMapped) {

        CSVParser parser;
        String[] headers;
        try {
            InputStream is =
                new ByteArrayInputStream(p_headers.getBytes("UTF-8"));
            parser = new CSVParser(is);

            // Parse the headers:
            parser.nextLine();
            headers = parser.getLineValues();

            if (headers == null || headers.length == 0) {
                _logException(null, null, "INVALID_FILE_MSG");
                return;
            }
        } catch (Exception e) {
            _logException(null, null, "INVALID_FILE_MSG");
            return;
        }

        int length = headers.length;

        String strColumn;
        CSVColumn column;
        _initValidColumns();
        Iterator<String> iter;
        Set<String> requiredColumns = new HashSet<String>();
        Set<String> existingColumns = new HashSet<String>();

        // Loop over the string of columns and create the column object
        for (int i = 0; i < length; i++) {
            strColumn = headers[i];
            if (strColumn != null) {
                strColumn = strColumn.trim();
                if (existingColumns.contains(strColumn)) {
                    Map<String, Object> paramMap =
                        new HashMap<String, Object>();
                    paramMap.put("COLUMN", strColumn);
                    _logException(1, i, "DUPLICATE_COLUMN_MSG", paramMap);
                    continue;
                }
                existingColumns.add(strColumn);
                column = new CSVColumn(strColumn, i);
                if (!column.process(m_validColumns)) {
                    _logError(1, i, "INVALID_COLUMN_MSG");
                    m_state.addColumn(column, -1);
                } else {
                    int iType = m_validColumns.get(column.getBaseName());
                    if (!m_state.addColumn(column, iType)) {
                        _logError(1, i, "INVALID_COLUMN_MSG");
                    }
                    if (m_state.isSingleCurrency() &&
                        AMOUNT_CURRENCY.equals(column.getBaseName())) {
                        _logException(1, i, "COLUMN_SINGLE_CURRENCY_MSG");
                    }
                }
            } else {
                m_state.addNullColumn();
            }
        }

        int totalBuckets = 0;
        if (p_preMapped) {
            totalBuckets = 3;
        } else {
            totalBuckets = m_currencyBuckets.size();
        }
        Iterator<Integer> intIter;

        intIter = m_state.getBucketIndexes().iterator();
        while (intIter.hasNext()) {
            Integer index = intIter.next();
            if (index > totalBuckets) {
                column = m_state.getColumn(AMOUNT + index);
                if (column != null) {
                    _logException(1, column.getColumn(),
                                  "COLUMN_NOT_MATCH_BUCKET_MSG");
                }
                column = m_state.getColumn(AMOUNT_CURRENCY + index);
                if (column != null) {
                    _logException(1, column.getColumn(),
                                  "COLUMN_NOT_MATCH_BUCKET_MSG");
                }
            }
        }


        Integer id;
        Iterator<Integer> idIter = m_state.getAttributeIndexes().iterator();
        while (idIter.hasNext()) {
            id = idIter.next();
            requiredColumns.add(ATTRIBUTE + id);
            requiredColumns.add(ATTRIBUTE_VALUE + id);
        }


        idIter = m_state.getReferenceIndexes().iterator();
        while (idIter.hasNext()) {
            id = idIter.next();
            requiredColumns.add(ATTACHMENT_NAME + id);
            requiredColumns.add(ATTACHMENT_TYPE + id);
        }

        idIter = m_state.getActionPlanAttributeIndexes().iterator();
        while (idIter.hasNext()) {
            id = idIter.next();
            requiredColumns.add(ACTION_PLAN_ATTRIBUTE + id);
            requiredColumns.add(ACTION_PLAN_ATTRIBUTE_VALUE + id);
        }

        idIter = m_state.getActionPlanReferenceIndexes().iterator();
        while (idIter.hasNext()) {
            id = idIter.next();
            requiredColumns.add(ACTION_PLAN_ATTACHMENT_NAME + id);
            requiredColumns.add(ACTION_PLAN_ATTACHMENT_TYPE + id);
        }

        //Transaction attributes are not mandatory now
        //requiredColumns.add(TRANSACTION_CODE);
        //requiredColumns.add(OPEN_DATE);
        if (p_preMapped) {
          requiredColumns.add(RECONCILIATION_ACCOUNT_ID);
        }

        iter = requiredColumns.iterator();
        while (iter.hasNext()) {
            strColumn = iter.next();
            if (m_state.getColumn(strColumn) == null) {
                Map<String, Object> paramMap = new HashMap<String, Object>();
                paramMap.put("COLUMN", strColumn);
                _logException(1, null, "MISSING_COLUMN_MSG", paramMap);
            }
        }

        if (!m_state.errorOccurred()) {
            m_state.normalizeColumns();
        }
    }


    /**
     * Initializes the list of format attributes
     *
     * @param p_am
     * @param p_lReconciliationId
     * @param p_strType
     */
    private void _initFormatAttributes(ApplicationModule p_am,
                                       Long p_lReconciliationId,
                                       String p_strType,
                                       Boolean p_bActionPlan) {

        if (!p_bActionPlan) {
            m_formatTransAttributes =
                    new LinkedList<HashMap<String, Object>>();
            m_validTransAttributes = new HashSet<Long>();
        } else {
            m_formatActionPlanAttributes =
                    new LinkedList<HashMap<String, Object>>();
            m_validActionPlanAttributes = new HashSet<Long>();
        }

        List<String> lstTransType = new ArrayList<String>();
        lstTransType.add(p_strType);
        ViewObject attributes =
            TransactionManager.initFormatAttributeAssignmentsVO(p_am,
                                                                lstTransType,
                                                                p_lReconciliationId);
        Row row;
        String type;
        RowSetIterator rsi = attributes.createRowSetIterator(null);
        
        // Fix for Bug 21043770 - ROLE ACCESS TO TRANS ATTRIBUTES IS NOT BEING ENFORCED ON PREMAPPED TRANSACTION
        String roleCode = ModelConstants.ROLE_PREPARER_CODE;
        if (ModelUtils.isAdministrator()) {
            roleCode = ModelConstants.ROLE_ADMINISTRATOR_CODE;
        } else if (ModelUtils.isPowerUserOnly()) {
            roleCode = ModelConstants.ROLE_POWER_USER_CODE;
        }

        while (rsi.hasNext()) {
            row = rsi.next();
            Long id = (Long)row.getAttribute(VOConstants.VO_ATTRIBUTE_ID);

            HashMap<String, Object> attribute = new HashMap<String, Object>();
            attribute.put("ID", id);
            type = (String)row.getAttribute(VOConstants.VO_ATTRIBUTE_TYPE);
            attribute.put("TYPE", type);
            
            HashMap<String, Boolean> attributeAccessProperties =
                _getAttributeAccessProperties(row, roleCode);

            Boolean bEditable = attributeAccessProperties.get("EDITABLE");
            Boolean bDisplayable =
                attributeAccessProperties.get("DISPLAYABLE");
            Boolean bRequired = attributeAccessProperties.get("REQUIRED");
            
            attribute.put("DISPLAYABLE", bDisplayable);
            attribute.put("EDITABLE", bEditable);
            attribute.put("REQUIRED", bRequired);

            if (bDisplayable) {
                if (!p_bActionPlan) {
                    m_validTransAttributes.add(id);
                } else {
                    m_validActionPlanAttributes.add(id);
                }
            }

            attribute.put("ORDER",
                          row.getAttribute(VOConstants.VO_ATTRIBUTE_ORDER_SEQ));
            attribute.put("COPY_TO_PROFILE",
                          row.getAttribute(VOConstants.VO_ATTRIBUTE_VALUES_COPY_TO_PROFILE));
            attribute.put("NAME",
                          row.getAttribute(VOConstants.VO_ATTRIBUTE_NAME));
            if (AttributeManager.isTypeNumber(type)) {
                attribute.put("VALUE",
                              row.getAttribute(VOConstants.VO_ATTRIBUTE_VALUES_NUMBER));
            } else if (AttributeManager.isTypeList(type)) {
                attribute.put("VALUE",
                              row.getAttribute(VOConstants.VO_ATTRIBUTE_VALUES_LIST_CHOICE));
            } else if (AttributeManager.isTypeDate(type)) {
                attribute.put("VALUE",
                              row.getAttribute(VOConstants.VO_ATTRIBUTE_VALUES_DATE));
            } else {
                attribute.put("VALUE",
                              row.getAttribute(VOConstants.VO_ATTRIBUTE_VALUES_TEXT));
            }

            if (!p_bActionPlan) {
                m_formatTransAttributes.add(attribute);
            } else {
                m_formatActionPlanAttributes.add(attribute);
            }
        }
        rsi.closeRowSetIterator();
    }

    /**
     * Execute set attribute access rules to overwrite attribute role access.
     *
     * @param p_am
     * @param p_lReconciliationId
     * @param p_strType
     */
    private void _executeSetAttributeAccessRules(ApplicationModule p_am,
                                                 Long p_lRuleContext,
                                                 Long p_lReconciliationId,
                                                 Long p_lTransactionId,
                                                 String p_strType,
                                                 Boolean p_bActionPlan) {
        Iterator iter = null;
        if (p_bActionPlan) {
            iter = m_formatActionPlanAttributes.iterator();
        } else {
            iter = m_formatTransAttributes.iterator();
        }

        while (iter.hasNext()) {
            HashMap<String, Object> attribute =
                (HashMap<String, Object>)iter.next();
            Long lAttributeId = (Long)attribute.get("ID");
            AccessRuleResult access =
                (AccessRuleResult)ARMRuleEngine.getAttributeAccess(p_lRuleContext, p_lReconciliationId,
                                                                   lAttributeId,
                                                                   p_lTransactionId,
                                                                   p_strType,
                                                                   p_am);
            if (access != null) {
                boolean bIsDisplayable = (Boolean)attribute.get("DISPLAYABLE");
                boolean bIsEditable = (Boolean)attribute.get("EDITABLE");
                boolean bIsRequired = (Boolean)attribute.get("REQUIRED");
                String strAccessType = access.getAccess();
                //check for required
                if (ModelConstants.ACCESS_REQUIRED.equals(strAccessType)) {
                    bIsDisplayable = true;
                    bIsEditable = true;
                    bIsRequired = true;
                } else if (ModelConstants.ACCESS_ALLOW_EDITS.equals(strAccessType)) {
                    bIsDisplayable = true;
                    bIsEditable = true;
                    bIsRequired = false;
                } else if (ModelConstants.ACCESS_DO_NOT_DISPLAY.equals(strAccessType)) {
                    bIsDisplayable = false;
                    bIsEditable = false;
                    bIsRequired = false;
                }

                attribute.remove("EDITABLE");
                attribute.remove("REQUIRED");
                attribute.remove("DISPLAYABLE");
                attribute.put("EDITABLE", bIsEditable);
                attribute.put("REQUIRED", bIsRequired);
                attribute.put("DISPLAYABLE", bIsDisplayable);

                if (bIsDisplayable) {
                    if (p_bActionPlan) {
                        m_validActionPlanAttributes.add(lAttributeId);
                    } else {
                        m_validTransAttributes.add(lAttributeId);
                    }
                } else {
                    if (p_bActionPlan) {
                        m_validActionPlanAttributes.remove(lAttributeId);
                    } else {
                        m_validTransAttributes.remove(lAttributeId);
                    }
                }
            }
        }
    }
    
    /**
     * Writes back the attribute values
     *
     * @param p_attrValueVO
     * @param p_lObjectId
     * @param p_rows
     * @param p_iRow
     * @param bIsActionPlan
     */
    private void _generateAttributeValues(ApplicationModule p_am,
                                          ViewObject p_attrValueVO,
                                          List<Row> p_rows, Long p_lObjectId,
                                          int p_iRow, Boolean bIsActionPlan) {
        _generateAttributeValues(p_am, p_attrValueVO, p_rows, p_lObjectId, p_iRow, bIsActionPlan, false);
    }
    
    /**
     * Writes back the attribute values
     *
     * @param p_attrValueVO
     * @param p_lObjectId
     * @param p_rows
     * @param p_iRow
     * @param bIsActionPlan
     */
    private void _generateAttributeValues(ApplicationModule p_am,
                                          ViewObject p_attrValueVO,
                                          List<Row> p_rows, Long p_lObjectId,
                                          int p_iRow, Boolean bIsActionPlan, Boolean isPremapped) {

        ImportConverter converter = m_state.getConverter();
        Iterator<Integer> indexes = null;
        if (bIsActionPlan) {
            indexes = m_state.getActionPlanAttributeIndexes().iterator();
        } else {
            indexes = m_state.getAttributeIndexes().iterator();
        }

        HashMap<Long, AttributeDef> attributes =
            new HashMap<Long, AttributeDef>();
        HashMap<Long, Object> values = new HashMap<Long, Object>();
        String strAttributeHeader = null;
        String strAttributeValueHeader = null;
        if (bIsActionPlan) {
            strAttributeHeader = ACTION_PLAN_ATTRIBUTE;
            strAttributeValueHeader = ACTION_PLAN_ATTRIBUTE_VALUE;
        } else {
            strAttributeHeader = ATTRIBUTE;
            strAttributeValueHeader = ATTRIBUTE_VALUE;
        }

        while (indexes.hasNext()) {
            Integer index = indexes.next();
            String attribute = _getValue(strAttributeHeader + index);
            if (attribute != null) {
                try {
                    String attributeValue =
                        _getValue(strAttributeValueHeader + index);
                    AttributeDef attr =
                        converter.convertUserAttribute(attribute);
                    Object value =
                        converter.convertAttributeValue(attributeValue, attr);
                    attr.setIndex(index);
                    if (attr.getCalculationId() != null && value != null) {
                      _error(strAttributeHeader + index, p_iRow, "CALCULATION_NO_VALUE");
                    }
                    if (!isPremapped && values.containsKey(attr.getId())) {
                        _error(strAttributeHeader + index, p_iRow, "DUPLICATE_ATTRIBUTE_MSG");
                    } else {
                        if (bIsActionPlan) {
                            if (!isPremapped && !m_validActionPlanAttributes.contains(attr.getId())) {
                                _error(strAttributeHeader + index, p_iRow,
                                       "INVALID_ATTRIBUTE_FORMAT_MSG");
                            } else if (m_validActionPlanAttributes.contains(attr.getId())) {
                                attributes.put(attr.getId(), attr);
                                values.put(attr.getId(), value);
                            }
                        } else {
                            if (!isPremapped && !m_validTransAttributes.contains(attr.getId())) {
                                _error(strAttributeHeader + index, p_iRow,
                                       "INVALID_ATTRIBUTE_FORMAT_MSG");
                            } else if (m_validTransAttributes.contains(attr.getId())) {
                                attributes.put(attr.getId(), attr);
                                values.put(attr.getId(), value);
                            }
                        }
                    }


                } catch (ImportException e) {
                    _error(strAttributeHeader + index, p_iRow, e.getMessage());
                }
            }
        }

        if (!m_state.errorOccurred()) {
            Iterator<HashMap<String, Object>> iter = null;

            if (bIsActionPlan) {
                iter = m_formatActionPlanAttributes.iterator();
            } else {
                iter = m_formatTransAttributes.iterator();
            }

            while (iter.hasNext()) {
                HashMap<String, Object> formatAttr = iter.next();
                Long attrId = (Long)formatAttr.get("ID");

                // skip the standard attributes because they're handled separately
                if (AttributeManager.isStandardActionPlanAttribute(attrId) ||
                    AttributeManager.isStandardTransactionAttribute(attrId)) {
                    continue;
                }

                AttributeDef attribute = attributes.get(attrId);
                Row row =
                    p_attrValueVO.createAndInitRow(new NameValuePairs(new String[] { VOConstants.VO_OBJECT_ID,
                                                                                     VOConstants.VO_ATTRIBUTE_ID,
                                                                                     VOConstants.VO_ATTRIBUTE_ORDER_SEQ,
                                                                                     VOConstants.VO_ATTRIBUTE_VALUES_REQUIRED,
                                                                                     VOConstants.VO_HISTORY_ATTRIBUTE_TYPE},
                                                                      new Object[] { p_lObjectId,
                                                                                     attrId,
                                                                                     formatAttr.get("ORDER"),
                                                                                     formatAttr.get("REQUIRED"),
                                                                                     "TXN"
                                                                                     }));
                //row.setAttribute(VOConstants.VO_HISTORY_ATTRIBUTE_TYPE, "TXN");
                p_rows.add(row);
                Object value;
                Boolean bEditable = (Boolean)formatAttr.get("EDITABLE");
                if (attribute == null || !bEditable || values.get(attribute.getId()) == null) {
                    value = formatAttr.get("VALUE");
                } else {
                    value = values.get(attribute.getId());
                }

                String type = (String)formatAttr.get("TYPE");
                if (value != null) {
                    if ((VOConstants.VO_ATTRIBUTE_TEXT_TYPE.equals(type) ||
                         VOConstants.VO_ATTRIBUTE_YESNO_TYPE.equals(type) ||
                         VOConstants.VO_ATTRIBUTE_TRUEFALSE_TYPE.equals(type) ||
                         VOConstants.VO_ATTRIBUTE_USER_TYPE.equals(type)) ||
                        VOConstants.VO_ATTRIBUTE_MULTILINE_TEXT_TYPE.equals(type)) {
                        _writeData(row, VOConstants.VO_ATTRIBUTE_VALUES_TEXT,
                                   value);
                    } else if ((VOConstants.VO_ATTRIBUTE_NUMBER_TYPE.equals(type) ||
                                VOConstants.VO_ATTRIBUTE_INTEGER_TYPE.equals(type))) {
                        _writeData(row, VOConstants.VO_ATTRIBUTE_VALUES_NUMBER,
                                   value);
                    } else if ((VOConstants.VO_ATTRIBUTE_DATE_TYPE.equals(type) ||
                                VOConstants.VO_ATTRIBUTE_DATETIME_TYPE.equals(type))) {
                        _writeData(row, VOConstants.VO_ATTRIBUTE_VALUES_DATE,
                                   value);
                    } else if (VOConstants.VO_ATTRIBUTE_LIST_TYPE.equals(type)) {
                        _writeData(row,
                                   VOConstants.VO_ATTRIBUTE_VALUES_LIST_CHOICE,
                                   value);
                    }
                }
            }
        }
    }

    /**
     * Generates the references
     */
    private Integer _generateReferences(ViewObject p_refVO, List<Row> p_rows,
                                     Long p_lObjectId, int p_iRow,
                                     REFERENCES_TYPES p_enumRefType) {

        ImportConverter converter = m_state.getConverter();
        Iterator<Integer> indexes = null;
        List<ReferenceDef> references = new LinkedList<ReferenceDef>();
        HashSet<String> referencesDone = new HashSet<String>();
        String strAttachmentNameHeader = null;
        String strAttachmentURLHeader = null;
        String strAttachmentTypeHeader = null;
        String strAttachmentDocIDHeader = null;
        Integer nAttachmentCount = 0;

        switch (p_enumRefType) {
        case REFERENCES:
            strAttachmentNameHeader = ATTACHMENT_NAME;
            strAttachmentURLHeader = ATTACHMENT_URL;
            strAttachmentTypeHeader = ATTACHMENT_TYPE;
            strAttachmentDocIDHeader = ATTACHMENT_DOC_ID;
            indexes = m_state.getReferenceIndexes().iterator();
            break;
        case ACTION_PLAN_REFERENCES:
            strAttachmentNameHeader = ACTION_PLAN_ATTACHMENT_NAME;
            strAttachmentURLHeader = ACTION_PLAN_ATTACHMENT_URL;
            strAttachmentTypeHeader = ACTION_PLAN_ATTACHMENT_TYPE;
            strAttachmentDocIDHeader = ACTION_PLAN_ATTACHMENT_DOC_ID;
            indexes = m_state.getActionPlanReferenceIndexes().iterator();
            break;

        default:
            strAttachmentNameHeader = ATTACHMENT_NAME;
            strAttachmentURLHeader = ATTACHMENT_URL;
            strAttachmentTypeHeader = ATTACHMENT_TYPE;
            strAttachmentDocIDHeader = ATTACHMENT_DOC_ID;
            indexes = m_state.getReferenceIndexes().iterator();
        }

        while (indexes.hasNext()) {
            Integer index = indexes.next();
            String refName = _getValue(strAttachmentNameHeader + index);
            String refType = _getValue(strAttachmentTypeHeader + index);
            try {
                if (refType != null) {
                    refType = converter.convertReferenceType(refType);
                }

                if (refName == null && refType != null &&
                    !VOConstants.VO_REFERENCES_TYPE_DOCUMENT.equals(refType)) {
                    _error(strAttachmentNameHeader + index, p_iRow,
                           "VALUE_EXPECTED_MSG");
                } else if (refName != null && refType == null) {
                    _error(strAttachmentTypeHeader + index, p_iRow,
                           "VALUE_EXPECTED_MSG");
                } else if (refName != null || refType != null) {
                    if (refName != null && referencesDone.contains(refName)) {
                        _error(strAttachmentNameHeader + index, p_iRow,
                               "DUPLICATE_REFERENCE_MSG");
                    } else {
                        referencesDone.add(refName);
                    }
                    ReferenceDef reference =
                        new ReferenceDef(refName, refType);
                    reference.setIndex(index);
                    references.add(reference);
                    if (VOConstants.VO_REFERENCES_TYPE_URL.equals(refType)) {
                        String url = _getValue(strAttachmentURLHeader + index);
                        if (url == null) {
                            _error(strAttachmentURLHeader + index, p_iRow,
                                   "VALUE_EXPECTED_MSG");
                        } else {
                            try {
                                new URL(url);
                            } catch (MalformedURLException e) {
                                _error(strAttachmentURLHeader + index, p_iRow,
                                       "INVALID_URL_MSG");
                            }
                            reference.setURL(url);
                        }
                        _checkIsNull(strAttachmentDocIDHeader + index, p_iRow);
                    } else if (VOConstants.VO_REFERENCES_TYPE_DOCUMENT.equals(refType)) {
                        String docId = _getValue(strAttachmentDocIDHeader + index);
                        if (docId == null) {
                            _error(strAttachmentDocIDHeader + index, p_iRow,
                                   "VALUE_EXPECTED_MSG");
                        } else {
                            reference.setDocId(docId);
                        }
                        _checkIsNull(strAttachmentURLHeader + index, p_iRow);
                    }
                }
            } catch (ImportException e) {
                // This happens if the reference type is bad
                _error(strAttachmentTypeHeader + index, p_iRow,
                       e.getMessage());
            }
        }

        if (!m_state.errorOccurred()) {
            Collections.sort(references);
            Iterator<ReferenceDef> iter = references.iterator();
            String strUserId = ModelUtils.getUserID();
            Timestamp now =  CalendarUtils.convertToJBODomainDate(new java.util.Date());
            while (iter.hasNext()) {
                ReferenceDef reference = iter.next();

                ArrayList<String> lAttributeNames = new ArrayList<String>();
                ArrayList<Object> lAttributeValues = new ArrayList<Object>();                

               lAttributeNames.add(VOConstants.VO_OBJECT_ID);
               lAttributeNames.add(VOConstants.VO_REFERENCES_REFERENCE_NAME);
               lAttributeNames.add(VOConstants.VO_REFERENCES_REFERENCE_TYPE);
               lAttributeNames.add(VOConstants.VO_REFERENCES_CREATOR_ID);
               lAttributeNames.add(VOConstants.VO_REFERENCES_USER_CREATION_DATE);
               lAttributeNames.add(VOConstants.VO_REFERENCES_CREATOR_ROLE);
  
                lAttributeValues.add(p_lObjectId);
                lAttributeValues.add(reference.getName());
                lAttributeValues.add(reference.getType());
                lAttributeValues.add(strUserId);
                lAttributeValues.add(now);
                lAttributeValues.add(ModelUtils.getRoleCode(ModelUtils.getUserRole()));

                if (reference.getDocId() != null)
                {
                  lAttributeNames.add(VOConstants.VO_REFERENCES_DOCUMENT_ID);
                  lAttributeValues.add(reference.getDocId());
                }
                if (reference.getURL() != null)
                {
                  lAttributeNames.add(VOConstants.VO_REFERENCES_URL);
                  lAttributeValues.add(reference.getURL());
                }
                if (reference.getFileName() != null)
                {
                  lAttributeNames.add(VOConstants.VO_REFERENCES_FILE_NAME);
                  lAttributeValues.add(reference.getFileName());
                }
                if (reference.getMimeType() != null)
                {
                  lAttributeNames.add(VOConstants.VO_REFERENCES_MIMETYPE);
                  lAttributeValues.add(reference.getMimeType());
                }

                String[] attributesArray = new String[lAttributeNames.size()];
                lAttributeNames.toArray(attributesArray);
                Object[] valuesArray = new Object[lAttributeValues.size()];
                lAttributeValues.toArray(valuesArray);

                Row row =
                    p_refVO.createAndInitRow(new NameValuePairs(attributesArray,valuesArray));

                nAttachmentCount ++;
            }
        }
        
        return nAttachmentCount;
    }



    /**
     * Generates the Amortization
     */
    private Row _generateAmortization(ViewObject amortVo, Long transactionId, int iRow, Locale locale) {
        Row amortRow =
            amortVo.createAndInitRow(new NameValuePairs(new String[] { VOConstants.VO_AMORTIZATION_TRANSACTION_ID },
                                                        new Object[] { transactionId }));
        _processAmortizationMethod(amortRow,
                                   VOConstants.VO_AMORTIZATION_METHOD,
                                   iRow, AMORT_METHOD, false);
        String strAmortMethod =
            (String)amortRow.getAttribute(VOConstants.VO_AMORTIZATION_METHOD);
        if (null == strAmortMethod) {
            strAmortMethod =
                    "X"; // this is a fake method, to avoid an NPE later on
        }
        boolean bActual = "A".equals(strAmortMethod);
        // ignore half-month if the amortization method is not straight-line
        boolean bHalfMonthInUse = false;
        if (!"S".equals(strAmortMethod)) {
            _writeData(amortRow,
                       VOConstants.VO_AMORTIZATION_HALF_MONTH_CONVENTION,
                       "N");
        } else {
            _processYesNo(amortRow,
                          VOConstants.VO_AMORTIZATION_HALF_MONTH_CONVENTION,
                          iRow, AMORT_HALF_MONTH_CONVENTION,
                          false, false);
            bHalfMonthInUse =
                    "Y".equals(amortRow.getAttribute(VOConstants.VO_AMORTIZATION_HALF_MONTH_CONVENTION));
        }
  
        _processAmortPeriods(amortRow,
                             VOConstants.VO_AMORTIZATION_NUM_PERIODS,
                             iRow, AMORT_NUMBER_PERIODS,
                             bActual, bHalfMonthInUse);
        _processPeriod(amortRow,
                       VOConstants.VO_AMORTIZATION_START_PERIOD,
                       iRow, AMORT_START_PERIOD, bActual);
        boolean bAllowNullDate = !bActual;
        _processDate(amortRow,
                     VOConstants.VO_AMORTIZATION_START_DATE,
                     iRow, AMORT_START_DATE, bAllowNullDate, false);
        _processDate(amortRow,
                     VOConstants.VO_AMORTIZATION_END_DATE,
                     iRow, AMORT_END_DATE, bAllowNullDate, false);
        
        Iterator<CurrencyBucket> bucketIter =
            m_currencyBuckets.iterator();
        int iBucketIndex = -1;
        int iBucketCounter = 1;
        String strAttr = null;
        String strHeader = null;
        while (bucketIter.hasNext()) {
            CurrencyBucket bucket = bucketIter.next();
            iBucketIndex = bucket.getBucketOrder() - 1;
            switch (iBucketIndex) {
                case 1:
                    strAttr = VOConstants.VO_AMORTIZATION_ORIGINAL_AMOUNT_BUCKET1;
                    break;
                case 2:
                    strAttr = VOConstants.VO_AMORTIZATION_ORIGINAL_AMOUNT_BUCKET2;
                    break;
                case 3:
                    strAttr = VOConstants.VO_AMORTIZATION_ORIGINAL_AMOUNT_BUCKET3;
                    break;
            }
            strHeader = AMORT_ORIGINAL_AMOUNT + iBucketCounter++;
            _processNumber(amortRow, strAttr, iRow, strHeader, locale);
            iBucketIndex++;
        }
        
        return amortRow;
    }
    
    /**
     * Generates the Action Plan
     */
    private void _generateActionPlan(ApplicationModule p_am, ViewObject transActionPlanVo, 
                                     Long transactionId, int iRow, List<Row> rows, boolean isPremapped) {

        
        Object planName = _processStandardAttribute(QueryConstants.QA_ARM_TRANS_ACT_PLAN, true,
                                  null, VOConstants.VO_TRANSACTION_ACTION_PLAN_NAME,
                                  iRow, ACTION_PLAN_NAME, 4000, QueryAttribute.ATTR_TYPE_TEXT, true);
        
        // action plan closed
        Object planClosed =_processStandardAttribute(QueryConstants.QA_ARM_TRANS_ACT_CLOSED, true,
                                  null, VOConstants.VO_TRANSACTION_ACTION_PLAN_CLOSED,
                                  iRow, ACTION_PLAN_NAME_CLOSED, null, QueryAttribute.ATTR_TYPE_YESNO, true);
        
        Object planCloseDate =_processStandardAttribute(QueryConstants.QA_ARM_TRANS_ACT_CLOSE_DATE, true,
                                  null, VOConstants.VO_TRANSACTION_ACTION_PLAN_CLOSE_DATE,
                                  iRow, ACTION_PLAN_CLOSE_DATE, null, QueryAttribute.ATTR_TYPE_DATE, true);


      Row rowActionPlan =
          transActionPlanVo.createAndInitRow(new NameValuePairs(new String[] { 
                                                      VOConstants.VO_TRANSACTION_ACTION_PLAN_OBJECT_ID ,
                                                      VOConstants.VO_TRANSACTION_ACTION_PLAN_NAME,
                                                      VOConstants.VO_TRANSACTION_ACTION_PLAN_CLOSED,
                                                      VOConstants.VO_TRANSACTION_ACTION_PLAN_CLOSE_DATE
                                                      },
                                                    new Object[] { 
                                                      transactionId,
                                                      planName,
                                                      planClosed,
                                                      planCloseDate
                                                    }));

      Long lActionPlanId =
          (Long)rowActionPlan.getAttribute(VOConstants.VO_TRANSACTION_ACTION_PLAN_ACTION_PLAN_ID);
        
        _generateAttributeValues(p_am,
                                 p_am.findViewObject(VOConstants.VO_ATTRIBUTE_VALUE_ASSIGNMENTSVO),
                                 rows, lActionPlanId, iRow,
                                 true, isPremapped);
        _generateReferences(p_am.findViewObject(VOConstants.VO_REFERENCES),
                            rows, lActionPlanId, iRow,
                            REFERENCES_TYPES.ACTION_PLAN_REFERENCES);
    }
    
    
    /**
     * Checks Prevent Save and Required Attachment rules
     */
    private void _checkPreventSaveAndReqAttachmentRules(ApplicationModule p_am,
                                                          boolean bHasPreventSaveRule, boolean bHasReqAttachmentRule,
                                                          Long reconciliationId, Long transactionId, String transactionType, 
                                                          int nAttachmentCount, int iRow,
                                                          Long p_lRuleContext) {
        if (bHasPreventSaveRule || bHasReqAttachmentRule){
            // post transaction details to DB to execute prevent transaction save rules
            p_am.getTransaction().postChanges();                        
        }
        if (bHasReqAttachmentRule){
            // execute require transaction attachment rules
            RuleResult resultReqAttachment =
                ARMRuleEngine.isTransactionAttachmentRequired(p_lRuleContext, reconciliationId,
                                                              transactionId,
                                                              transactionType,
                                                              p_am);
            if ((resultReqAttachment != null) &&
                resultReqAttachment.isPositive() &&
                nAttachmentCount == 0) {
                _logTranslatedError(iRow, null,
                                    resultReqAttachment.getMessage());
                m_bError = true;
            }                        
        }                    
        if (bHasPreventSaveRule) {
            // execute prevent transaction save rules
            RuleResult resultNotSavable =
                ARMRuleEngine.isTransactionNotSaveable(p_lRuleContext, reconciliationId,
                                                       transactionId,
                                                       transactionType,
                                                       p_am);
            if ((resultNotSavable != null) &&
                resultNotSavable.isPositive()) {
                _logTranslatedError(iRow, null,
                                    resultNotSavable.getMessage());
                m_bError = true;
            }
        }
    }
    
    
    /**
     * Returns the data for the column, if it exists
     * @param p_strColumn
     * @return
     */
    private String _getValue(String p_strColumn) {
        CSVColumn column = m_state.getColumn(p_strColumn);
        if (column != null && m_data.length > column.getColumn()) {
            return m_data[column.getColumn()];
        }
        return null;
    }


    /**
     * Processes a textual value
     *
     * @param p_row
     * @param p_strAttribute
     * @param p_iRow
     * @param p_strColumn
     * @param p_iMaxLength
     * @param p_bAllowNull
     */
    private String _processText(Row p_row, String p_strAttribute, int p_iRow,
                              String p_strColumn, Integer p_iMaxLength,
                              boolean p_bAllowNull, boolean p_bBatch) {
        String value = _getValue(p_strColumn);
        if (value != null) {
            if (p_iMaxLength != null && value.length() > p_iMaxLength) {
                _error(p_strColumn, p_iRow, "VALUE_CANNOT_EXCEED", "SIZE",
                       p_iMaxLength);
                return null;
            }
            if (!p_bBatch) {
              _writeData(p_row, p_strAttribute, value);
            }
            return value;
        } else if (!p_bAllowNull) {
            _error(p_strColumn, p_iRow, "VALUE_EXPECTED_MSG");
        }
        return null;
    }


    /**
     * Processes a boolean value
     *
     * @param p_row - row of data to write to
     * @param p_strAttribute - row attribute name
     * @param p_iRow - number of the row (for error messages)
     * @param p_strColumn - letter of the column (for error messages)
     * @param p_bAllowNull - does this field allow NULL or not?
     * 
     * @return String (Y, N or error)
     */
    private String _processYesNo(Row p_row, String p_strAttribute, int p_iRow,
                               String p_strColumn, boolean p_bAllowNull, boolean p_bBatch) {
        String value = _getValue(p_strColumn);
        if (value != null) {
            try {
                String strValue = m_state.getConverter().convertYesNo(value);
                if (!p_bBatch) {
                  _writeData(p_row, p_strAttribute, strValue);
                }
                return strValue;
            } catch (ImportException e) {
                _error(p_strColumn, p_iRow, e.getMessage());
            }
        } else if (!p_bAllowNull) {
            _error(p_strColumn, p_iRow, "VALUE_EXPECTED_MSG");
            return null;
        }
        return null;
    }

    /**
     * Processes a text value that is None, Amortizing or Accreting
     *
     * @param p_row
     * @param p_strAttribute
     * @param p_iRow
     * @param p_strColumn
     * @param p_bAllowNull
     */
    private String _processAmortization(Row p_row, String p_strAttribute,
                                        int p_iRow, String p_strColumn,
                                        boolean p_bAllowNull) {
        String value = _getValue(p_strColumn);
        if (value != null) {
            try {
                String strValue =
                    m_state.getConverter().convertAmortization(value);
                _writeData(p_row, p_strAttribute, strValue);
                return strValue;
            } catch (ImportException e) {
                _error(p_strColumn, p_iRow, e.getMessage());
            }
        } else if (!p_bAllowNull) {
            _error(p_strColumn, p_iRow, "VALUE_EXPECTED_MSG");
            return null;
        }
        return null;
    }

    /**
     * Processes a text value that is Actual, Custom or Straightline
     *
     * @param p_row
     * @param p_strAttribute
     * @param p_iRow
     * @param p_strColumn
     * @param p_bAllowNull
     */
    private void _processAmortizationMethod(Row p_row, String p_strAttribute,
                                            int p_iRow, String p_strColumn,
                                            boolean p_bAllowNull) {
        String value = _getValue(p_strColumn);
        if (value != null) {
            try {
                String strValue =
                    m_state.getConverter().convertAmortizationMethod(value);
                _writeData(p_row, p_strAttribute, strValue);
            } catch (ImportException e) {
                _error(p_strColumn, p_iRow, e.getMessage());
            }
        } else if (!p_bAllowNull) {
            _error(p_strColumn, p_iRow, "VALUE_EXPECTED_MSG");
            return;
        }
    }


    /**
     * Processes a date value
     *
     * @param p_row
     * @param p_strAttribute
     * @param p_iRow
     * @param p_strColumn
     * @param p_bAllowNull
     */
    private Date _processDate(Row p_row, String p_strAttribute, int p_iRow,
                              String p_strColumn, boolean p_bAllowNull, boolean p_bBatch) {
        String value = _getValue(p_strColumn);
        if (value != null) {
            try {
                Date dValue = m_state.getConverter().convertDate(value);
                if (!p_bBatch) {
                  _writeData(p_row, p_strAttribute, dValue);
                }
                return dValue;            
            } catch (ImportException e) {
                _error(p_strColumn, p_iRow, e.getMessage());
            }
        } else if (!p_bAllowNull) {
            _error(p_strColumn, p_iRow, "VALUE_EXPECTED_MSG");
        }
        return null;        
    }


    /**
     * Processes a number
     *
     * @param p_row
     * @param p_strAttribute
     * @param p_iRow
     * @param p_strColumn
     * @param p_locale
     */
    private void _processNumber(Row p_row, String p_strAttribute, int p_iRow,
                                String p_strColumn, Locale p_locale) {
        String value = _getValue(p_strColumn);
        if (value != null) {
            try {
                BigDecimal fValue =
                    m_state.getConverter().convertFormattedFloat(value,
                                                                 p_locale);
                _writeData(p_row, p_strAttribute, fValue);
            } catch (ImportException e) {
                _error(p_strColumn, p_iRow, e.getMessage());
            }
        }
    }

    /**
     * Processes the amortization periods
     *
     * @param p_row
     * @param p_strAttribute
     * @param p_iRow
     * @param p_strColumn
     * @param p_bAllowNull
     */
    private void _processAmortPeriods(Row p_row, String p_strAttribute,
                                      int p_iRow, String p_strColumn,
                                      boolean p_bAllowNull,
                                      boolean p_bHalfMonthInUse) {
        String value = _getValue(p_strColumn);
        if (value != null) {
            Integer iValue = null;
            try {
              iValue = new Integer(value);
            }
            catch (NumberFormatException e) {
                iValue = -1; // to trip the exception below
            }

            if (iValue.intValue() <= 0) {
                _error(p_strColumn, p_iRow, "INVALID_NUMBER_OF_PERIODS_MSG");
            } else {
                if (p_bHalfMonthInUse && iValue.intValue() <= 1) {
                    _error(p_strColumn, p_iRow,
                           "INVALID_AMORT_HALF_MONTH_PERIODS_MSG");
                } else {
                    _processInteger(p_row, p_strAttribute, p_iRow,
                                    p_strColumn);
                }
            }
        } else {
            if (!p_bAllowNull) {
                _error(p_strColumn, p_iRow, "VALUE_EXPECTED_MSG");
            }
        }
    }

    /**
     * Processes an integer
     *
     * @param p_row
     * @param p_strAttribute
     * @param p_iRow
     * @param p_strColumn
     */
    private void _processInteger(Row p_row, String p_strAttribute, int p_iRow,
                                 String p_strColumn) {
        String value = _getValue(p_strColumn);
        if (value != null) {
            try {
                Integer iValue = m_state.getConverter().convertInteger(value);
                _writeData(p_row, p_strAttribute, iValue);
            } catch (NumberFormatException e) {
                _error(p_strColumn, p_iRow, "INVALID_INTEGER_MSG");
            }
        }
    }

    /**
     * Processes a Period Name
     *
     * @param p_row
     * @param p_strAttribute
     * @param p_iRow
     * @param p_strColumn
     * @paran p_bAllowNull - is NULL a valid value
     */
    private void _processPeriod(Row p_row, String p_strAttribute, int p_iRow,
                                String p_strColumn, boolean p_bAllowNull) {
        String value = _getValue(p_strColumn);
        if (value != null) {
            try {
                Long lValue = m_state.getConverter().convertPeriodName(value);
                _writeData(p_row, p_strAttribute, lValue);
            } catch (ImportException e) {
                _error(p_strColumn, p_iRow, e.getMessage());
            }
        } else {
            if (!p_bAllowNull) {
                _error(p_strColumn, p_iRow, "VALUE_EXPECTED_MSG");
            }
        }
    }

    /**
     * Processes a currency value
     *
     * @param p_row
     * @param p_strAttribute
     * @param p_iRow
     * @param p_strColumn
     */
    private void _processCurrency(Row p_row, String p_strAttribute, int p_iRow,
                                  String p_strColumn, boolean p_bAllowNull) {
        String value = _getValue(p_strColumn);
        if (value != null) {
            if (m_state.getConverter().isCurrencyValid(value)) {
                _writeData(p_row, p_strAttribute, value);
            } else {
                _error(p_strColumn, p_iRow, "INVALID_CURRENCY_MSG");
            }
        } else if (!p_bAllowNull) {
            _error(p_strColumn, p_iRow, "VALUE_EXPECTED_MSG");
            return;
        }
    }


    /**
     * Writes data to the row
     *
     * @param p_row
     * @param p_strAttribute
     * @param p_value
     */
    private void _writeData(Row p_row, String p_strAttribute, Object p_value) {
        // Only write back if the value has changed:
        Object oldValue = p_row.getAttribute(p_strAttribute);
        if ((p_value != null && !p_value.equals(oldValue)) ||
            (p_value == null && oldValue != null)) {
            p_row.setAttribute(p_strAttribute, p_value);
        }
    }


    /**
     * Checks that the column is null, and if not, logs an error
     * @param p_strColumn
     */
    private void _checkIsNull(String p_strColumn, int p_iRow) {
        if (_getValue(p_strColumn) != null) {
            _error(p_strColumn, p_iRow, "UNEXPECTED_VALUE_MSG");
        }
    }


    /**
     * Wrapper around logging an error
     * @param p_strColumn
     * @param p_iRow
     * @param p_strMessage
     */
    private void _error(String p_strColumn, int p_iRow, String p_strMessage) {
        Integer iCol = null;
        CSVColumn col = m_state.getColumn(p_strColumn);
        if (col != null) {
            iCol = col.getColumn();
        }
        m_bError = true;
        _logError(p_iRow, iCol, p_strMessage);
    }


    /**
     * Wrapper around logging an error
     * @param p_strColumn
     * @param p_iRow
     * @param p_strMessage
     */
    private void _error(String p_strColumn, int p_iRow, String p_strMessage,
                        String p_strToken, Object p_value) {
        Integer iCol = null;
        CSVColumn col = m_state.getColumn(p_strColumn);
        if (col != null) {
            iCol = col.getColumn();
        }
        HashMap<String, Object> params = new HashMap<String, Object>();
        params.put(p_strToken, p_value);
        m_bError = true;
        _logError(p_iRow, iCol, p_strMessage, params);
    }


    /**
     * Generates a list of valid columns and the column type
     */
    private static synchronized void _initValidColumns() {
        if (m_validColumns != null) {
            return;
        }
        m_validColumns = new HashMap<String, Integer>(33);
        m_validColumns.put(RECONCILIATION_ACCOUNT_ID, COLUMN_TYPE_TRANSACTION);
        m_validColumns.put(TRANSACTION_CODE, COLUMN_TYPE_TRANSACTION);
        m_validColumns.put(TRANSACTION_DESCRIPTION, COLUMN_TYPE_TRANSACTION);
        m_validColumns.put(OPEN_DATE, COLUMN_TYPE_TRANSACTION);
        m_validColumns.put(CLOSE_DATE, COLUMN_TYPE_TRANSACTION);
        m_validColumns.put(FROM_FDMEE, COLUMN_TYPE_TRANSACTION);
        m_validColumns.put(AMORT_ACCRETE, COLUMN_TYPE_TRANSACTION);

        // amortization
        m_validColumns.put(AMORT_METHOD, COLUMN_TYPE_TRANSACTION);
        m_validColumns.put(AMORT_HALF_MONTH_CONVENTION,
                           COLUMN_TYPE_TRANSACTION);
        m_validColumns.put(AMORT_NUMBER_PERIODS, COLUMN_TYPE_TRANSACTION);
        m_validColumns.put(AMORT_START_PERIOD, COLUMN_TYPE_TRANSACTION);
        m_validColumns.put(AMORT_START_DATE, COLUMN_TYPE_TRANSACTION);
        m_validColumns.put(AMORT_END_DATE, COLUMN_TYPE_TRANSACTION);
        m_validColumns.put(AMORT_ORIGINAL_AMOUNT, COLUMN_TYPE_TRANSACTION);

        m_validColumns.put(AMOUNT, COLUMN_TYPE_BUCKET);
        m_validColumns.put(AMOUNT_CURRENCY, COLUMN_TYPE_BUCKET);

        m_validColumns.put(ATTACHMENT_NAME, COLUMN_TYPE_REFERENCE);
        m_validColumns.put(ATTACHMENT_TYPE, COLUMN_TYPE_REFERENCE);
        m_validColumns.put(ATTACHMENT_URL, COLUMN_TYPE_REFERENCE);
        m_validColumns.put(ATTACHMENT_DOC_ID, COLUMN_TYPE_REFERENCE);

        m_validColumns.put(ATTRIBUTE, COLUMN_TYPE_ATTRIBUTE);
        m_validColumns.put(ATTRIBUTE_VALUE, COLUMN_TYPE_ATTRIBUTE);

        m_validColumns.put(ACTION_PLAN_NAME, COLUMN_TYPE_ACTION_PLAN);
        m_validColumns.put(ACTION_PLAN_NAME_CLOSED, COLUMN_TYPE_ACTION_PLAN);
        m_validColumns.put(ACTION_PLAN_CLOSE_DATE, COLUMN_TYPE_ACTION_PLAN);

        m_validColumns.put(ACTION_PLAN_ATTACHMENT_NAME,
                           COLUMN_TYPE_ACTION_PLAN_REFERENCE);
        m_validColumns.put(ACTION_PLAN_ATTACHMENT_TYPE,
                           COLUMN_TYPE_ACTION_PLAN_REFERENCE);
        m_validColumns.put(ACTION_PLAN_ATTACHMENT_URL,
                           COLUMN_TYPE_ACTION_PLAN_REFERENCE);
        m_validColumns.put(ACTION_PLAN_ATTACHMENT_DOC_ID,
                           COLUMN_TYPE_ACTION_PLAN_REFERENCE);

        m_validColumns.put(ACTION_PLAN_ATTRIBUTE,
                           COLUMN_TYPE_ACTION_PLAN_ATTRIBUTE);
        m_validColumns.put(ACTION_PLAN_ATTRIBUTE_VALUE,
                           COLUMN_TYPE_ACTION_PLAN_ATTRIBUTE);
    }

    /**
     * returns format attribute action plan type
     *
     * @param p_strTransactionType - Transaction Type
     */
    private String _getActionPlanType(String p_strTransactionType) {
        String strActionType = null;
        if (ARMmodelConstants.FORMAT_ATTRIBUTE_TYPE_BALANCE_EXPLANATION.equals(p_strTransactionType)) {
            strActionType =
                    ARMmodelConstants.FORMAT_ATTRIBUTE_TYPE_BALANCE_EXPLANATION_ACTION;
        } else if (ARMmodelConstants.FORMAT_ATTRIBUTE_TYPE_SOURCE_SYSTEM_BALANCE.equals(p_strTransactionType)) {
            strActionType =
                    ARMmodelConstants.FORMAT_ATTRIBUTE_TYPE_SOURCE_SYSTEM_BALANCE_ACTION;
        } else if (ARMmodelConstants.FORMAT_ATTRIBUTE_TYPE_SUBSYSTEM_BALANCE.equals(p_strTransactionType)) {
            strActionType =
                    ARMmodelConstants.FORMAT_ATTRIBUTE_TYPE_SUBSYSTEM_BALANCE_ACTION;
        }
        return strActionType;
    }

    /**
     * Returns true if the Role Access view accessor returns a displayable role
     *  access the object's roles
     *
     * @param p_row  - row of a VO that contains the RoleAccessVA
     * @param p_strUserRole - P, A, R1, R2, etc
     *
     * @return boolean
     */
    private static HashMap<String, Boolean> _getAttributeAccessProperties(Row p_row,
                                                                          String p_strUserRole) {
        HashMap<String, Boolean> attributeAccessProperties =
            new HashMap<String, Boolean>(3);

        boolean bIsDisplayable = true;
        boolean bIsEditable = false;
        boolean bIsRequired = false;

        RowSet rsRoleAccess = (RowSet)p_row.getAttribute("RoleAccessVA");
        rsRoleAccess.reset();
        while (rsRoleAccess.hasNext()) {
            Row rowRoleAccess = rsRoleAccess.next();
            String strAccess =
                (String)rowRoleAccess.getAttribute(VOConstants.VO_ROLE_ACCESS_ACCESS_TYPE);
            String strRole =
                (String)rowRoleAccess.getAttribute(VOConstants.VO_ROLE_ACCESS_ROLE_NAME);
            String strObjectType =
                (String)rowRoleAccess.getAttribute("ObjectType");
            if (p_strUserRole.equals(strRole)) {
                if ("V".equals(strObjectType)) {
                    //check for required
                    if (ModelConstants.ACCESS_REQUIRED.equals(strAccess)) {
                        bIsEditable = true;
                        bIsRequired = true;
                    } else if (ModelConstants.ACCESS_ALLOW_EDITS.equals(strAccess)) { //check for edit
                        bIsEditable = true;
                    }

                    //check for display
                    if (!ModelUtils.isRoleAccessDisplayable(strAccess)) {
                        bIsDisplayable = false;
                    }
                }
            }
        }

        attributeAccessProperties.put("DISPLAYABLE", bIsDisplayable);
        attributeAccessProperties.put("EDITABLE", bIsEditable);
        attributeAccessProperties.put("REQUIRED", bIsRequired);

        return attributeAccessProperties;
    }


    public static Boolean isActionPlanDisplayed(ApplicationModule p_am,
                                                Long p_lFormatId,
                                                String p_strTransType) {
        Boolean bShowActionPlan = false;
        ViewObject vo =
            CommonManager.executeQueryWithCriteria(p_am, VOConstants.VVO_FORMAT,
                                                   VOConstants.VVO_FORMAT_BY_FORMAT_ID_CRITERIA,
                                                   VOConstants.VVO_BIND_FORMAT_ID,
                                                   p_lFormatId);
        if (vo != null) {
            Row row = vo.next();
            if (row != null) {
                String strShowActionPlanDisplay = null;
                if (ARMmodelConstants.FORMAT_ATTRIBUTE_TYPE_BALANCE_EXPLANATION.equals(p_strTransType)) {
                    strShowActionPlanDisplay =
                            (String)row.getAttribute(VOConstants.VO_FORMAT_SHOW_ACTION_PLAN_BEX);
                } else if (ARMmodelConstants.FORMAT_ATTRIBUTE_TYPE_SOURCE_SYSTEM_BALANCE.equals(p_strTransType)) {
                    strShowActionPlanDisplay =
                            (String)row.getAttribute(VOConstants.VO_FORMAT_SHOW_ACTION_PLAN_SRC);
                } else if (ARMmodelConstants.FORMAT_ATTRIBUTE_TYPE_SUBSYSTEM_BALANCE.equals(p_strTransType)) {
                    strShowActionPlanDisplay =
                            (String)row.getAttribute(VOConstants.VO_FORMAT_SHOW_ACTION_PLAN_SUB);
                }
                bShowActionPlan = "Y".equals(strShowActionPlanDisplay);
            }
        }
        return bShowActionPlan;
    }

    /**
     * Given an attribute id, check if it's editable by the current user
     *
     * @param p_lAttributeId - attribute id
     * @param p_bActionPlan - true if it's an action plan attribute
     *
     * @return boolean
     */
    private Object _processStandardAttribute(Long p_lAttributeId,
                                          boolean p_bActionPlan,
                                          Row p_row, 
                                          String p_strAttribute, 
                                          int p_iRow,
                                          String p_strColumn, 
                                          Integer p_iMaxLength,
                                          String p_strType,
                                          boolean p_bBatch) {
     
        Iterator<HashMap<String, Object>> iter =
            p_bActionPlan ? m_formatActionPlanAttributes.iterator() :
            m_formatTransAttributes.iterator();

        boolean bEditable = false;
        Object defValue = null;
        Object writtenValue = null;
        
        while (iter.hasNext()) {
            HashMap<String, Object> formatAttr = iter.next();
            Long lId = (Long)formatAttr.get("ID");
            if (p_lAttributeId.equals(lId)) {
                bEditable = (Boolean)formatAttr.get("EDITABLE");
                defValue = formatAttr.get("VALUE");
                break;
            }
        }
        
        if (bEditable) {
          if (QueryAttribute.ATTR_TYPE_DATE.equals(p_strType)) {
            writtenValue = _processDate(p_row, p_strAttribute, p_iRow, p_strColumn, true, p_bBatch);
          } else if (QueryAttribute.ATTR_TYPE_YESNO.equals(p_strType)) {
            writtenValue = _processYesNo(p_row, p_strAttribute, p_iRow, p_strColumn, true, p_bBatch);
          } else {
            writtenValue = _processText(p_row, p_strAttribute,
                         p_iRow, p_strColumn, p_iMaxLength, true, p_bBatch);
          }
        }
        //if (defValue != null && p_row.getAttribute(p_strAttribute) == null) {
        if ((writtenValue == null) && (defValue != null)) {
          writtenValue = defValue;
          if (!p_bBatch) {
            p_row.setAttribute(p_strAttribute, defValue);
          }
        }
      return writtenValue;
    }
    
     /**
      * Delete transaction and its related values (attribute values, comments, references, and action plan).
      * It returns number of transactions deleted.
      */
     private int _deleteAllTransactions(ApplicationModuleImpl p_am, Long p_lPeriodId, String p_strTransactionType, String strReconIdList) {
         // Add single quote to query parameter
         p_strTransactionType = "'"+p_strTransactionType+"'";
         
         // Get number of transactions deleting and return this count
         String countDelete = "select count(transaction_id) as Count\n" + 
         "                     from ARM_TRANSACTIONS \n" + 
         "                     where upper(RECONCILIATION_ACCOUNT_ID) IN (?) and \n" + 
         "                           PERIOD_ID = ? and \n" + 
         "                           TRANSACTION_TYPE = ? and \n" + 
         "                           FROM_FLAG = 'P'";
         
         countDelete = ModelUtils.replaceAll(countDelete, "PERIOD_ID = ?", "PERIOD_ID = "+p_lPeriodId);
         countDelete = ModelUtils.replaceAll(countDelete, "TRANSACTION_TYPE = ?", "TRANSACTION_TYPE = "+p_strTransactionType);
         countDelete = ModelUtils.replaceAll(countDelete, "(?)", "("+strReconIdList+")");
         ModelLogger.logFine("Select Statement after replacing parameters : " + countDelete);
         
         int count = 0;
         PreparedStatement countStatement = null;
         try {
             countStatement = p_am.getDBTransaction().createPreparedStatement(countDelete, 0);
             ResultSet result = countStatement.executeQuery();
             while (result.next()){
                count = result.getInt("Count");
             }
             countStatement.close();
             //p_am.getDBTransaction().commit();
         } catch (SQLException e) {
             //p_am.getDBTransaction().rollback();
             ModelLogger.logException(e);
             _logException(null, null, e);
             return 0;
         }
         
         if (count == 0) {
             // There is no transactions to delete but need to clean workflow action table
             String deleteWorkflowActionStatement = "delete from ARM_WORKFLOW_ACTIONS where OBJECT_ID IN \n" + 
             "      (select recon.RECONCILIATION_ID from ARM_RECONCILIATIONS recon\n" + 
             "      where upper(recon.RECONCILIATION_ACCOUNT_ID) IN (?) and \n" + 
             "            recon.PERIOD_ID = ? and \n" + 
             "            recon.STATUS_ID in (1, 10))";
             deleteWorkflowActionStatement = ModelUtils.replaceAll(deleteWorkflowActionStatement, "PERIOD_ID = ?", "PERIOD_ID = "+p_lPeriodId);
             deleteWorkflowActionStatement = ModelUtils.replaceAll(deleteWorkflowActionStatement, "(?)", "("+strReconIdList+")");
             ModelLogger.logFine("DELETE statement after replacing parameters : " + deleteWorkflowActionStatement);
                     
             PreparedStatement deleteStatement = null;
             try {
                 deleteStatement = p_am.getDBTransaction().createPreparedStatement(deleteWorkflowActionStatement, 0);
                 deleteStatement.execute();
                 deleteStatement.close();
             } catch (SQLException e) {
                 ModelLogger.logException(e);
                 _logException(null, null, e);
                 return 0;
             }
             
             return 0;
         }
         
         //
         // Delete transactions and all its related artifacts
         //
         String deleteReferencesStatement = "delete from ARM_REFERENCES\n" + 
         "                   where OBJECT_ID IN (select TRANSACTION_ID\n" + 
         "                   from ARM_TRANSACTIONS\n" + 
         "                   where upper(RECONCILIATION_ACCOUNT_ID) IN (?) and\n" + 
         "                         PERIOD_ID = ? and\n" +
         "                         TRANSACTION_TYPE = ? and \n" +
         "                         FROM_FLAG = 'P')\n" + 
         "      or\n" + 
         "      OBJECT_ID IN (select COMMENT_ID\n" + 
         "                   from ARM_COMMENTS\n" + 
         "                   where OBJECT_ID IN (select TRANSACTION_ID\n" + 
         "                                      from ARM_TRANSACTIONS\n" + 
         "                                      where upper(RECONCILIATION_ACCOUNT_ID) IN (?) and\n" + 
         "                                            PERIOD_ID = ? and\n" + 
         "                                            TRANSACTION_TYPE = ? and \n" +
         "                                            FROM_FLAG = 'P')\n" + 
         "                          or\n" + 
         "                          OBJECT_ID IN  (select ACTION_PLAN_ID\n" + 
         "                                        from ARM_ACTION_PLAN\n" + 
         "                                        where OBJECT_ID IN (select TRANSACTION_ID\n" + 
         "                                                           from ARM_TRANSACTIONS\n" + 
         "                                                           where upper(RECONCILIATION_ACCOUNT_ID) IN (?) and\n" + 
         "                                                                 PERIOD_ID = ? and\n" + 
         "                                                                 TRANSACTION_TYPE = ? and \n" +
         "                                                                 FROM_FLAG = 'P')))\n" + 
         "      or\n" + 
         "      OBJECT_ID IN (select ACTION_PLAN_ID\n" + 
         "                   from ARM_ACTION_PLAN\n" + 
         "                   where OBJECT_ID IN (select TRANSACTION_ID\n" + 
         "                                      from ARM_TRANSACTIONS\n" + 
         "                                      where upper(RECONCILIATION_ACCOUNT_ID) IN (?) and\n" + 
         "                                            PERIOD_ID = ? and\n" + 
         "                                            TRANSACTION_TYPE = ? and \n" +
         "                                            FROM_FLAG = 'P')); \n";

         String deleteCommentsStatement = "delete from ARM_COMMENTS\n" + 
         "                   where OBJECT_ID IN (select TRANSACTION_ID\n" + 
         "                   from ARM_TRANSACTIONS\n" + 
         "                   where upper(RECONCILIATION_ACCOUNT_ID) IN (?) and\n" + 
         "                         PERIOD_ID = ? and\n" + 
         "                         TRANSACTION_TYPE = ? and \n" +
         "                         FROM_FLAG = 'P')\n" + 
         "      or\n" + 
         "      OBJECT_ID IN (select ACTION_PLAN_ID\n" + 
         "                   from ARM_ACTION_PLAN\n" + 
         "                   where OBJECT_ID IN (select TRANSACTION_ID\n" + 
         "                                      from ARM_TRANSACTIONS\n" + 
         "                                      where upper(RECONCILIATION_ACCOUNT_ID) IN (?) and\n" + 
         "                                            PERIOD_ID = ? and\n" + 
         "                                            TRANSACTION_TYPE = ? and \n" +
         "                                            FROM_FLAG = 'P')); \n";
         
         String deleteAttributeValuesStatement = "delete from ARM_ATTRIBUTE_VALUES\n" + 
         "                   where OBJECT_ID IN (select TRANSACTION_ID\n" + 
         "                   from ARM_TRANSACTIONS\n" + 
         "                   where upper(RECONCILIATION_ACCOUNT_ID) IN (?) and\n" + 
         "                         PERIOD_ID = ? and\n" + 
         "                         TRANSACTION_TYPE = ? and \n" +
         "                         FROM_FLAG = 'P')\n" + 
         "      or\n" + 
         "      OBJECT_ID IN (select ACTION_PLAN_ID\n" + 
         "                   from ARM_ACTION_PLAN\n" + 
         "                   where OBJECT_ID IN (select TRANSACTION_ID\n" + 
         "                                      from ARM_TRANSACTIONS\n" + 
         "                                      where upper(RECONCILIATION_ACCOUNT_ID) IN (?) and\n" + 
         "                                            PERIOD_ID = ? and\n" + 
         "                                            TRANSACTION_TYPE = ? and \n" +
         "                                            FROM_FLAG = 'P')); \n";
         
         String deleteActionPlansStatement = "delete from ARM_ACTION_PLAN\n" + 
         "                   where OBJECT_ID IN (select TRANSACTION_ID\n" + 
         "                   from ARM_TRANSACTIONS\n" + 
         "                   where upper(RECONCILIATION_ACCOUNT_ID) IN (?) and\n" + 
         "                         PERIOD_ID = ? and\n" + 
         "                         TRANSACTION_TYPE = ? and \n" +
         "                         FROM_FLAG = 'P'); \n";
         
         String deleteTransactionAmtsStatement = "delete from ARM_TRANSACTION_AMOUNTS\n" + 
         "                   where OBJECT_ID IN (select TRANSACTION_ID\n" + 
         "                   from ARM_TRANSACTIONS\n" + 
         "                   where upper(RECONCILIATION_ACCOUNT_ID) IN (?) and\n" + 
         "                         PERIOD_ID = ? and\n" + 
         "                         TRANSACTION_TYPE = ? and \n" +
         "                         FROM_FLAG = 'P'); \n";
         
         String deleteTransactionsStatement = "delete from ARM_TRANSACTIONS\n" + 
         "      where upper(RECONCILIATION_ACCOUNT_ID) IN (?) and\n" + 
         "      PERIOD_ID = ? and\n" + 
         "      TRANSACTION_TYPE = ? and \n" +
         "      FROM_FLAG = 'P'; \n";
         
         String deleteWorkflowActionStatement = "delete from ARM_WORKFLOW_ACTIONS where OBJECT_ID IN \n" + 
         "      (select recon.RECONCILIATION_ID from ARM_RECONCILIATIONS recon\n" + 
         "      where upper(recon.RECONCILIATION_ACCOUNT_ID) IN (?) and \n" + 
         "            recon.PERIOD_ID = ? and \n" + 
         "            recon.STATUS_ID in (1, 10)); \n";
         
         StringBuffer sbDeleteQuery = new StringBuffer("BEGIN \n");
         sbDeleteQuery.append(deleteReferencesStatement);
         sbDeleteQuery.append(deleteCommentsStatement);
         sbDeleteQuery.append(deleteAttributeValuesStatement);
         sbDeleteQuery.append(deleteActionPlansStatement);
         sbDeleteQuery.append(deleteTransactionAmtsStatement);
         sbDeleteQuery.append(deleteTransactionsStatement);
         sbDeleteQuery.append(deleteWorkflowActionStatement);
         sbDeleteQuery.append("END; \n");
         
         // Replace query input parameters
         String statement = sbDeleteQuery.toString();
         statement = ModelUtils.replaceAll(statement, "PERIOD_ID = ?", "PERIOD_ID = "+p_lPeriodId);
         statement = ModelUtils.replaceAll(statement, "TRANSACTION_TYPE = ?", "TRANSACTION_TYPE = "+p_strTransactionType);
         statement = ModelUtils.replaceAll(statement, "(?)", "("+strReconIdList+")");
         ModelLogger.logFine("DELETE statement after replacing parameters : " + statement);
                 
         PreparedStatement deleteStatement = null;
         try {
             deleteStatement = p_am.getDBTransaction().createPreparedStatement(statement, 0);
             deleteStatement.execute();
             deleteStatement.close();
             //p_am.getDBTransaction().commit();
         } catch (SQLException e) {
             //p_am.getDBTransaction().rollback();
             ModelLogger.logException(e);
             _logException(null, null, e);
             return 0;
         }
         
         return count;
     }
     
    /**
     * Validate if current user has access to the given reconciliation id
     * 
     * @return Boolean
     */
    private Boolean _validateUserSecurityScope(ApplicationModule p_am, Long p_lReconciliationId) {
        Boolean bValid = Boolean.FALSE;
        if (ModelUtils.isAdministrator()) {
            ModelLogger.logFine("Transacction Importing user is Administrator");
            // the current user is admin, so no scope checking
            bValid = Boolean.TRUE;
        } else if (ModelUtils.isPowerUserOnly()){
            ModelLogger.logFine("Transacction Importing user is Power User");
            ARMAttributeList list = ARMAttributeList.getAttributeList(p_am);
            List<QueryAttribute> acctSegments = list.getProfileAccountSegments();
            HashMap<QueryAttribute, Object> values = new HashMap<QueryAttribute, Object>(acctSegments.size());
            ViewObject voSegValues = CommonManager.executeQueryWithCriteria(p_am,
                                               VOConstants.VO_ACCOUNT_SEGMENT_VALUES,
                                               "AccountSegmentsByObjectIdCriteria",
                                               VOConstants.VO_BIND_OBJECT_ID,
                                               p_lReconciliationId);

            RowSetIterator rsi = voSegValues.createRowSetIterator(null);
            int nIndex = 0;
            while (rsi.hasNext()) {
                Row row = rsi.next();
                String strType = (String)row.getAttribute(VOConstants.VO_ATTRIBUTE_TYPE);
                Object oValue = null;
                if (VOConstants.VO_ATTRIBUTE_INTEGER_TYPE.equalsIgnoreCase(strType)) {
                    oValue = row.getAttribute(VOConstants.VO_ATTRIBUTE_VALUES_NUMBER);
                } else if (VOConstants.VO_ATTRIBUTE_LIST_TYPE.equalsIgnoreCase(strType)) {
                    oValue = row.getAttribute(VOConstants.VO_ATTRIBUTE_VALUES_LIST_CHOICE);
                } else if (VOConstants.VO_ATTRIBUTE_TEXT_TYPE.equalsIgnoreCase(strType)) {
                    oValue = row.getAttribute(VOConstants.VO_ATTRIBUTE_VALUES_TEXT);
                }
                values.put(acctSegments.get(nIndex), oValue);
                nIndex++;
            }

            bValid = FilterManager.evaluatePowerUserSecurity(p_am, values);
            rsi.closeRowSetIterator();
            voSegValues.clearCache();
        } else if (ModelUtils.isPreparer()) {
            ModelLogger.logFine("Transacction Importing user is Preparer");
            // the current user is preparer, check if the user has access to the reconciliation
            String strCurrentUserId = ModelUtils.getUserID();
            List<String> groups = IdentityCache.getUserAffiliationsIds(strCurrentUserId, p_am);
            HashMap<String, Object> vcBindVars = new HashMap<String, Object>(2);
            vcBindVars.put(VOConstants.VO_ACCESS_BIND_OBJECT_ID, p_lReconciliationId);
            vcBindVars.put(VOConstants.VO_ACCESS_BIND_ACCESS_TYPE, "P");

            ViewObject voAccess = CommonManager.executeQueryWithCriteria(p_am, VOConstants.VO_ACCESS, VOConstants.VO_ACCESS_USER_BY_OBJECT_ID_AND_TYPE_CRITERIA, vcBindVars);

            RowSetIterator rsi = voAccess.createRowSetIterator(null);
            while (rsi.hasNext())
            {
                try {
                    Row row = rsi.next();
                    String strUserId = (String) row.getAttribute("UserId");
                    ModelLogger.logFine("strCurrentUserId is " + strCurrentUserId);
                    ModelLogger.logFine("strUserId is " + strUserId);
                    if (strCurrentUserId.equals(strUserId))
                    {
                        ModelLogger.logFine("strCurrentUserId is preparer. Return true");
                        bValid = Boolean.TRUE;
                        break;
                    } 
                    
                    // Fix for 20994940 and 20995066
                    // Check if current user is part of Group or Team assigned to reconciliation preparer
                    if (strUserId != null) {
                        for (String id : groups) {
                            ModelLogger.logFine("id is " + id);
                            if (id.equals(strUserId)) {
                                ModelLogger.logFine("strUserId is team or group. Return true");
                                bValid = Boolean.TRUE;
                                break;
                            }
                        }
                        
                    }
                    
                    // also check backup users (could be null)
                    String strBackupUserId = (String) row.getAttribute("BackupUserId");
                    ModelLogger.logFine("strBackupUserId is " + strBackupUserId);
                    if (strCurrentUserId.equals(strBackupUserId))
                    {
                        // Fix for Bug 20994763
                        // Main user is available so import should be done from main user (not backup user)
                        CSSUserIF[] users = IdentityCache.findUsersFromLoginId(strUserId);
                        if (users != null && users.length > 0 &&
                            IdentityCache.userHasPreparerRole(strUserId)){
                            ModelLogger.logError("Main user is available but backup user is trying to importing. Return false");
                            bValid = Boolean.FALSE;
                            break;
                        }
                        
                        Identity identity = IdentityCache.getGroup(strUserId);
                        if (identity != null && !identity.isInvalidId()) {
                            ModelLogger.logError("Main user group is available but backup user is trying to importing. Return false");
                            bValid = Boolean.FALSE;
                            break;
                        }
                        identity = IdentityCache.getTeam(strUserId);
                        if (identity != null && !identity.isInvalidId()) {
                            ModelLogger.logError("Main team is available but backup user is trying to importing. Return false");
                            bValid = Boolean.FALSE;
                            break;
                        }
                        
                        ModelLogger.logFine("strCurrentUserId is valid backup preparer. Return true");
                        bValid = Boolean.TRUE;
                        break;
                    }
                    
                    
                } catch(Exception e) {
                    _logException(null, null, e);
                    return Boolean.FALSE;
                }
            }
            rsi.closeRowSetIterator();
            voAccess.clearCache();
        }

        return bValid;
    }
}
