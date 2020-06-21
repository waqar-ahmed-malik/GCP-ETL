SET NOCOUNT ON;

CREATE TABLE ##tmpitems
  (
     itemrowid                       INT IDENTITY(1, 1),
     applyrowid                      INT NOT NULL,
     uniqreceipt                     INT NOT NULL,
     uniqreceiptline                 INT NOT NULL,
     uniqdisbursement                INT NOT NULL,
     uniqdisbursementline            INT NOT NULL,
     uniqjournalentry                INT NOT NULL,
     uniqjournalentryline            INT NOT NULL,
     uniqvoucher                     INT NOT NULL,
     uniqvoucherline                 INT NOT NULL,
     uniqglaccountbalancelink        INT NOT NULL,
     entrytype                       CHAR(4) COLLATE database_default NOT NULL,
     refernumber                     INT NULL,
     uniqglaccount                   INT NOT NULL,
     effectivedate                   DATETIME NULL,
     uniqentity                      INT NOT NULL,
     accountingmonth                 VARCHAR(6) COLLATE database_default NOT
     NULL,
     entrydescription                VARCHAR(125) COLLATE database_default NOT
     NULL,
     detaildescription               VARCHAR(125) COLLATE database_default NOT
     NULL,
     uniqagency                      INT NOT NULL,
     uniqbranch                      INT NOT NULL,
     uniqdepartment                  INT NOT NULL,
     uniqprofitcenter                INT NOT NULL,
     reversed                        VARCHAR(1) COLLATE database_default NOT
     NULL,
     creditamount                    NUMERIC(19, 4) NULL,
     debitamount                     NUMERIC(19, 4) NULL,
     checknumber                     VARCHAR(20) COLLATE database_default NULL,
     inserteddate                    DATETIME NULL,
     insertedbycode                  VARCHAR(20) COLLATE database_default NULL,
     glschedulecode                  VARCHAR(10) COLLATE database_default NULL,
     voidnumber                      VARCHAR(20) COLLATE database_default NULL,
     linetype                        CHAR COLLATE database_default NOT NULL,
     detailapplyto                   VARCHAR(8) COLLATE database_default NOT
     NULL,
     applytocode                     CHAR(1) COLLATE database_default NULL,
     uniqbankaccount                 INT NOT NULL,
     paymentgroupnumber              INT NOT NULL,
     linenumber                      INT NOT NULL,
     uniqrecurringentry              INT NOT NULL,
     uniqpolicyreserve               INT NOT NULL,
     uniqlinereserve                 INT NOT NULL,
     uniqentityreserve               INT NOT NULL,
     uniqallocationmethod            INT NOT NULL,
     uniqallocationstructuregrouping INT NOT NULL,
     payee                           VARCHAR(125) COLLATE database_default NULL,
     routing                         VARCHAR(50) COLLATE database_default NULL,
     comments                        VARCHAR(4000) COLLATE database_default NULL
  ); ;

SELECT  receipt.uniqreceipt,
        receiptline.uniqreceiptline
INTO   ##tmpreceipts
FROM   receipt
       INNER JOIN receiptline
               ON receipt.uniqreceipt = receiptline.uniqreceipt
WHERE  CONVERT(DATE, receipt.INSERTEDDATE) = CONVERT(DATE, GETDATE() - xxx_replace_this_xxx);

INSERT INTO ##tmpitems
SELECT ApplyRowID = -1,
       receipt.uniqreceipt,
       receiptline.uniqreceiptline,
       UniqDisbursement = -1,
       UniqDisbursementLine = -1,
       UniqJournalEntry = -1,
       UniqJournalEntryLine = -1,
       UniqVoucher = -1,
       UniqVoucherLine = -1,
       UniqGLAccountBalanceLink = Isnull((SELECT Max(
       glaccountbalancelink.uniqglaccountbalancelink)
        FROM   glaccountbalancelink
        WHERE  glaccountbalancelink.uniqreceipt =
               receiptline.uniqreceipt
               AND
       glaccountbalancelink.uniqreceiptline =
       receiptline.uniqreceiptline
               AND receipt.uniqreceipt <> -1
               AND receiptline.uniqreceiptline <> -1)
                                  , -1),
       EntryType = CASE
                     WHEN receiptline.uniqreceiptvoid = -1 THEN 'R'
                     WHEN receipt.uniqreceipt < v.uniqreceipt THEN 'R'
                     ELSE 'RV'
                   END,
       ReferNumber = CONVERT(VARCHAR(20), Space(20-Len(receipt.refernumber))
                                          + Rtrim(receipt.refernumber)),
       receiptline.uniqglaccount,
       receipt.effectivedate,
       receiptline.uniqentity,
       receiptline.accountingmonth,
       EntryDescription = receipt.descriptionof,
       DetailDescription = receiptline.descriptionof,
       receiptline.uniqagency,
       receiptline.uniqbranch,
       receiptline.uniqdepartment,
       receiptline.uniqprofitcenter,
       Reversed = '',
       CreditAmount = CASE
                        WHEN receiptline.amount < 0 THEN (
                        receiptline.amount * -1 )
                        ELSE 0
                      END,
       DebitAmount = CASE
                       WHEN receiptline.amount >= 0 THEN receiptline.amount
                       ELSE 0
                     END,
       CheckNumber = receiptline.paymentid,
       receiptline.inserteddate,
       receiptline.insertedbycode,
       receiptline.glschedulecode,
       VoidNumber = v.refernumber,
       LineType = CASE
                    WHEN receiptline.receiptlinenumber = 1 THEN 'F'
                    ELSE 'D'
                  END,
       DetailApplyTo = 'Detail',
       receiptline.applytocode,
       UniqBankAccount = receipt.uniqglaccountbankaccount,
       receiptline.paymentgroupnumber,
       LineNumber = receiptline.receiptlinenumber,
       UniqRecurringEntry = -1,
       receiptline.uniqpolicyreserve,
       receiptline.uniqlinereserve,
       receiptline.uniqentityreserve,
       receiptline.uniqallocationmethod,
       receiptline.uniqallocationstructuregrouping,
       Payee = NULL,
       Routing = NULL,
       Comments = NULL
FROM   receipt
       INNER JOIN receiptline
               ON receipt.uniqreceipt = receiptline.uniqreceipt
       INNER JOIN receipt v
               ON receiptline.uniqreceiptvoid = v.uniqreceipt
WHERE  receipt.uniqreceipt > -1
       AND receipt.uniqreceipt IN (SELECT ##tmpreceipts.uniqreceipt
                                   FROM   ##tmpreceipts);

SELECT disbursement.uniqdisbursement,
       disbursementline.uniqdisbursementline
INTO   ##tmpdisb
FROM   disbursement
       INNER JOIN disbursementline
               ON disbursement.uniqdisbursement =
                  disbursementline.uniqdisbursement
WHERE  (CONVERT(DATE, disbursement.INSERTEDDATE) = CONVERT(DATE, GETDATE() - xxx_replace_this_xxx));

INSERT INTO ##tmpitems
SELECT ApplyRowID = -1,
       UniqReceipt = -1,
       UniqReceiptLine = -1,
       disbursement.uniqdisbursement,
       disbursementline.uniqdisbursementline,
       UniqJournalEntry = -1,
       UniqJournalEntryLine = -1,
       UniqVoucher = -1,
       UniqVoucherLine = -1,
       UniqGLAccountBalanceLink = Isnull((SELECT Max(
       glaccountbalancelink.uniqglaccountbalancelink)
        FROM   glaccountbalancelink
        WHERE  glaccountbalancelink.uniqdisbursement
               =
               disbursementline.uniqdisbursement
               AND
       glaccountbalancelink.uniqdisbursementline =
       disbursementline.uniqdisbursementline
               AND disbursement.uniqdisbursement <>
                   -1
               AND
       disbursementline.uniqdisbursementline <> -1),
                                  -1),
       EntryType = CASE
                     WHEN disbursement.uniqdisbursementvoid = -1 THEN 'D'
                     WHEN disbursement.uniqdisbursement < v.uniqdisbursement
                   THEN 'D'
                     ELSE 'DV'
                   END,
       ReferNumber = CONVERT(VARCHAR(20),
                     Space(20-Len(disbursement.refernumber))
                     + Rtrim(disbursement.refernumber)),
       disbursementline.uniqglaccount,
       disbursement.effectivedate,
       UniqEntity = disbursement.uniqentitypayee,
       disbursement.accountingmonth,
       EntryDescription = disbursement.descriptionof,
       DetailDescription = disbursementline.descriptionof,
       disbursementline.uniqagency,
       disbursementline.uniqbranch,
       disbursementline.uniqdepartment,
       disbursementline.uniqprofitcenter,
       Reversed = '',
       CreditAmount = CASE
                        WHEN disbursementline.amount < 0 THEN (
                        disbursementline.amount * -1 )
                        ELSE 0
                      END,
       DebitAmount = CASE
                       WHEN disbursementline.amount >= 0 THEN
                       disbursementline.amount
                       ELSE 0
                     END,
       CheckNumber = disbursement.checknumber,
       disbursement.inserteddate,
       disbursement.insertedbycode,
       disbursementline.glschedulecode,
       VoidNumber = v.refernumber,
       LineType = CASE
                    WHEN disbursementline.linenumber = 1 THEN 'F'
                    ELSE 'D'
                  END,
       DetailApplyTo = 'Detail',
       disbursementline.applytocode,
       UniqBankAccount = disbursement.uniqglaccountbankaccount,
       PaymentGroupNumber = 0,
       LineNumber = disbursementline.linenumber,
       disbursement.uniqrecurringentry,
       disbursementline.uniqpolicyreserve,
       disbursementline.uniqlinereserve,
       disbursementline.uniqentityreserve,
       disbursementline.uniqallocationmethod,
       disbursementline.uniqallocationstructuregrouping,
       disbursement.payee,
       disbursement.routing,
       disbursement.comments
FROM   disbursement
       INNER JOIN disbursementline
               ON disbursement.uniqdisbursement =
                  disbursementline.uniqdisbursement
       INNER JOIN disbursement v
               ON disbursement.uniqdisbursementvoid = v.uniqdisbursement
WHERE  disbursement.uniqdisbursement > -1
       AND disbursement.uniqdisbursement IN (SELECT ##tmpdisb.uniqdisbursement
                                             FROM   ##tmpdisb)
       AND NOT ( disbursement.accountingmonth = v.accountingmonth );

SELECT a.uniqagency,
       FiscalInitialMonth = CASE
                              WHEN a.firstfiscalmonth = '01' THEN a.initialmonth
                              WHEN Substring(a.initialmonth, 5, 2) >=
                                   a.firstfiscalmonth THEN
                              CONVERT(CHAR(4), CONVERT(INT,
                              Substring(a.initialmonth, 1, 4)) + 1 )
                              + RIGHT('0' + CONVERT(VARCHAR(2), CONVERT(INT,
                              Substring(
                              a.initialmonth, 5, 2)
                              ) - CONVERT(INT, a.firstfiscalmonth) + 1 ), 2)
                              ELSE
       Substring(a.initialmonth, 1, 4)
       + RIGHT('0' + CONVERT(VARCHAR(2), (12
       - CONVERT(INT,
       a.firstfiscalmonth)
       + CONVERT(INT, Substring(a.initialmonth, 5, 2)) + 1)
       ), 2)
                            END,
       a.initialmonth,
       a.firstfiscalmonth,
       CriteriaStartYearMonth = 201904,
       BeginYearMonth = CONVERT(CHAR(4), CASE WHEN a.firstfiscalmonth = '01'
                        THEN 2019
                        WHEN 04 >=
                        CONVERT(INT, a.firstfiscalmonth) THEN 2019 + 1 ELSE 2019
                        END) +
                        RIGHT('0' + CONVERT(VARCHAR(2), CASE WHEN '04' >=
                        a.firstfiscalmonth THEN 04 - CONVERT(INT,
                        a.firstfiscalmonth) +
                        1 ELSE (12 -
                        a.firstfiscalmonth + 04 + 1) END), 2),
       EndYearMonth = CONVERT(CHAR(4), CASE WHEN a.firstfiscalmonth = '01' THEN
                      2019
                      WHEN 04 >=
                      CONVERT(INT, a.firstfiscalmonth) THEN 2019 + 1 ELSE 2019
                      END) +
                      RIGHT('0' + CONVERT(VARCHAR(2), CASE WHEN '04' >=
                      a.firstfiscalmonth THEN 04 - CONVERT(INT,
                      a.firstfiscalmonth) + 1
                      ELSE (12
                      - CONVERT(INT, a.firstfiscalmonth) + 04 + 1) END), 2),
       JanuaryMonthYear = CONVERT(CHAR(4), CASE WHEN a.firstfiscalmonth = '01'
                          THEN
                          2019 WHEN 04 >=
                          CONVERT(INT, a.firstfiscalmonth) THEN 2019 + 1 ELSE
                          2019 END)
                          + a.firstfiscalmonth,
       AnnualStart = CONVERT(CHAR(4), CASE WHEN a.firstfiscalmonth = '01' THEN
                     2019
                     WHEN 04 >=
                     CONVERT(INT, a.firstfiscalmonth) THEN 2019 + 1 ELSE 2019
                     END) +
                     '01',
       AnnualEnd = CONVERT(CHAR(4), CASE WHEN a.firstfiscalmonth = '01' THEN
                   2019 WHEN
                   04 >=
                   CONVERT(INT, a.firstfiscalmonth) THEN 2019 + 1 ELSE 2019 END)
                   + '12'
INTO   ##tmpagency
FROM   agency a
WHERE  a.uniqagency > -1; ;

SELECT journalentry.uniqjournalentry,
       journalentryline.uniqjournalentryline
INTO   ##tmpje
FROM   journalentry
       INNER JOIN journalentryline
               ON journalentry.uniqjournalentry =
                  journalentryline.uniqjournalentry
       INNER JOIN ##tmpagency
               ON journalentryline.uniqagency = ##tmpagency.uniqagency
WHERE  1 = 1
       AND ( ( journalentry.typeof IN ( 'J', 'M' )
               AND (( CONVERT(DATE, journalentry.INSERTEDDATE) = CONVERT(DATE, GETDATE() - xxx_replace_this_xxx) )) )
              OR ( journalentry.typeof = 'O'
                   AND ##tmpagency.fiscalinitialmonth >=
                       ##tmpagency.beginyearmonth
                   AND ##tmpagency.fiscalinitialmonth <=
                 ##tmpagency.endyearmonth )
              OR ( journalentry.typeof = 'Y'
                   AND journalentry.accountingmonth >= ##tmpagency.beginyearmonth
                   AND journalentry.accountingmonth <=
           ##tmpagency.endyearmonth ) );

INSERT INTO ##tmpitems
SELECT ApplyRowID = -1,
       UniqReceipt = -1,
       UniqReceiptLine = -1,
       UniqDisbursement = -1,
       UniqDisbursementLine = -1,
       journalentry.uniqjournalentry,
       journalentryline.uniqjournalentryline,
       UniqVoucher = -1,
       UniqVoucherLine = -1,
       UniqGLAccountBalanceLink = Isnull((SELECT Max(
       glaccountbalancelink.uniqglaccountbalancelink)
        FROM   glaccountbalancelink
        WHERE  glaccountbalancelink.uniqjournalentry
               =
               journalentry.uniqjournalentry
               AND
       glaccountbalancelink.uniqjournalentryline =
       journalentryline.uniqjournalentryline
               AND journalentry.uniqjournalentry <>
                   -1
               AND
       journalentryline.uniqjournalentryline <> -1),
                                  -1),
       EntryType = CASE
                     WHEN journalentry.typeof = 'M' THEN
                       CASE
                         WHEN journalentry.uniqjournalentryvoid = -1 THEN 'MJE'
                         WHEN journalentry.uniqjournalentry <
                              journalentry.uniqjournalentryvoid
                       THEN
                         'MJE'
                         ELSE 'MJEV'
                       END
                     WHEN journalentry.typeof = 'O' THEN 'O'
                     WHEN journalentry.typeof = 'Y' THEN 'YJE'
                     ELSE
                       CASE
                         WHEN journalentry.uniqjournalentryvoid = -1 THEN 'J'
                         WHEN journalentry.uniqjournalentry <
                              journalentry.uniqjournalentryvoid
                       THEN
                         'J'
                         ELSE 'JV'
                       END
                   END,
       ReferNumber = CONVERT(VARCHAR(20),
                     Space(20-Len(journalentry.refernumber))
                     + Rtrim(journalentry.refernumber)),
       journalentryline.uniqglaccount,
       journalentry.effectivedate,
       journalentry.uniqentity,
       journalentry.accountingmonth,
       EntryDescription = journalentry.descriptionof,
       DetailDescription = journalentryline.descriptionof,
       journalentryline.uniqagency,
       journalentryline.uniqbranch,
       journalentryline.uniqdepartment,
       journalentryline.uniqprofitcenter,
       Reversed = CASE
                    WHEN journalentry.uniqjournalentryreverse > -1 THEN '~'
                    ELSE ''
                  END,
       CreditAmount = CASE
                        WHEN journalentryline.amount < 0 THEN (
                        journalentryline.amount * -1 )
                        ELSE 0
                      END,
       DebitAmount = CASE
                       WHEN journalentryline.amount >= 0 THEN
                       journalentryline.amount
                       ELSE 0
                     END,
       CheckNumber = NULL,
       journalentry.inserteddate,
       journalentry.insertedbycode,
       journalentryline.glschedulecode,
       VoidNumber = v.refernumber,
       LineType = 'F',
       DetailApplyTo = 'Detail',
       ApplyToCode = NULL,
       UniqBankAccount = journalentryline.uniqglaccount,
       PaymentGroupNumber = 0,
       LineNumber = journalentryline.linenumber,
       journalentry.uniqrecurringentry,
       UniqPolicyReserve = -1,
       UniqLineReserve = -1,
       UniqEntityReserve = -1,
       journalentryline.uniqallocationmethod,
       journalentryline.uniqallocationstructuregrouping,
       Payee = NULL,
       Routing = NULL,
       Comments = NULL
FROM   journalentry
       INNER JOIN journalentryline
               ON journalentry.uniqjournalentry =
                  journalentryline.uniqjournalentry
       INNER JOIN journalentry v
               ON journalentry.uniqjournalentryvoid = v.uniqjournalentry
       INNER JOIN glaccount ba
               ON journalentryline.uniqglaccount = ba.uniqglaccount
WHERE  journalentryline.uniqjournalentryline > -1
       AND ba.flags & 8 = 8
       AND journalentry.uniqjournalentry IN (SELECT ##tmpje.uniqjournalentry
                                             FROM   ##tmpje)
       AND ( journalentry.typeof = 'J'
              OR journalentry.typeof = 'M'
              OR journalentry.typeof = 'Y'
              OR journalentry.typeof = 'O' )
       AND journalentry.typeof IN ( 'J', 'M', 'O', 'Y' )
       AND NOT ( journalentry.accountingmonth = v.accountingmonth );

INSERT INTO ##tmpitems
SELECT ApplyRowID = -1,
       UniqReceipt = -1,
       UniqReceiptLine = -1,
       UniqDisbursement = -1,
       UniqDisbursementLine = -1,
       journalentry.uniqjournalentry,
       journalentryline.uniqjournalentryline,
       UniqVoucher = -1,
       UniqVoucherLine = -1,
       UniqGLAccountBalanceLink = Isnull((SELECT Max(
       glaccountbalancelink.uniqglaccountbalancelink)
        FROM   glaccountbalancelink
        WHERE  glaccountbalancelink.uniqjournalentry
               =
               journalentry.uniqjournalentry
               AND
       glaccountbalancelink.uniqjournalentryline =
       journalentryline.uniqjournalentryline
               AND journalentry.uniqjournalentry <>
                   -1
               AND
       journalentryline.uniqjournalentryline <> -1),
                                  -1),
       EntryType = CASE
                     WHEN journalentry.typeof = 'M' THEN
                       CASE
                         WHEN journalentry.uniqjournalentryvoid = -1 THEN 'MJE'
                         WHEN journalentry.uniqjournalentry <
                              journalentry.uniqjournalentryvoid
                       THEN
                         'MJE'
                         ELSE 'MJEV'
                       END
                     WHEN journalentry.typeof = 'O' THEN 'O'
                     WHEN journalentry.typeof = 'Y' THEN 'YJE'
                     ELSE
                       CASE
                         WHEN journalentry.uniqjournalentryvoid = -1 THEN 'J'
                         WHEN journalentry.uniqjournalentry <
                              journalentry.uniqjournalentryvoid
                       THEN
                         'J'
                         ELSE 'JV'
                       END
                   END,
       ReferNumber = CONVERT(VARCHAR(20),
                     Space(20-Len(journalentry.refernumber))
                     + Rtrim(journalentry.refernumber)),
       journalentryline.uniqglaccount,
       journalentry.effectivedate,
       journalentry.uniqentity,
       journalentry.accountingmonth,
       EntryDescription = journalentry.descriptionof,
       DetailDescription = journalentryline.descriptionof,
       journalentryline.uniqagency,
       journalentryline.uniqbranch,
       journalentryline.uniqdepartment,
       journalentryline.uniqprofitcenter,
       Reversed = CASE
                    WHEN journalentry.uniqjournalentryreverse > -1 THEN '~'
                    ELSE ''
                  END,
       CreditAmount = CASE
                        WHEN journalentryline.amount < 0 THEN (
                        journalentryline.amount * -1 )
                        ELSE 0
                      END,
       DebitAmount = CASE
                       WHEN journalentryline.amount >= 0 THEN
                       journalentryline.amount
                       ELSE 0
                     END,
       CheckNumber = NULL,
       journalentry.inserteddate,
       journalentry.insertedbycode,
       journalentryline.glschedulecode,
       VoidNumber = v.refernumber,
       LineType = 'D',
       DetailApplyTo = 'Detail',
       ApplyToCode = NULL,
       UniqBankAccount = Isnull((SELECT Min(je.uniqbankaccount)
                                 FROM   ##tmpitems je
                                 WHERE  je.uniqjournalentry =
                                        journalentry.uniqjournalentry
                                        AND je.uniqjournalentry > -1), -1),
       PaymentGroupNumber = 0,
       LineNumber = journalentryline.linenumber,
       journalentry.uniqrecurringentry,
       UniqPolicyReserve = -1,
       UniqLineReserve = -1,
       UniqEntityReserve = -1,
       journalentryline.uniqallocationmethod,
       journalentryline.uniqallocationstructuregrouping,
       Payee = NULL,
       Routing = NULL,
       Comments = NULL
FROM   journalentry
       INNER JOIN journalentryline
               ON journalentry.uniqjournalentry =
                  journalentryline.uniqjournalentry
       INNER JOIN journalentry v
               ON journalentry.uniqjournalentryvoid = v.uniqjournalentry
       INNER JOIN glaccount nba
               ON journalentryline.uniqglaccount = nba.uniqglaccount
WHERE  journalentryline.uniqjournalentryline > -1
       AND nba.flags & 8 = 0
       AND journalentry.uniqjournalentry IN (SELECT ##tmpje.uniqjournalentry
                                             FROM   ##tmpje)
       AND ( journalentry.typeof = 'J'
              OR journalentry.typeof = 'M'
              OR journalentry.typeof = 'Y'
              OR journalentry.typeof = 'O' )
       AND journalentry.typeof IN ( 'J', 'M', 'O', 'Y' )
       AND NOT ( journalentry.accountingmonth = v.accountingmonth );

SELECT voucher.uniqvoucher,
       voucherline.uniqvoucherline
INTO   ##tmpvoucher
FROM   voucher
       INNER JOIN voucherline
               ON voucher.uniqvoucher = voucherline.uniqvoucher
WHERE  (( CONVERT(DATE, voucher.INSERTEDDATE) = CONVERT(DATE, GETDATE() - xxx_replace_this_xxx) ));

INSERT INTO ##tmpitems
SELECT ApplyRowID = -1,
       UniqReceipt = -1,
       UniqReceiptLine = -1,
       UniqDisbursement = -1,
       UniqDisbursementLine = -1,
       UniqJournalEntry = -1,
       UniqJournalEntryLine = -1,
       voucher.uniqvoucher,
       voucherline.uniqvoucherline,
       UniqGLAccountBalanceLink = Isnull((SELECT Max(
       glaccountbalancelink.uniqglaccountbalancelink)
        FROM   glaccountbalancelink
        WHERE  glaccountbalancelink.uniqvoucher =
               voucherline.uniqvoucher
               AND
       glaccountbalancelink.uniqvoucherline =
       voucherline.uniqvoucherline
               AND voucher.uniqvoucher <> -1
               AND voucherline.uniqvoucherline <> -1)
                                  , -1),
       EntryType = CASE
                     WHEN voucher.uniqvouchervoid = -1 THEN 'V'
                     WHEN voucher.uniqvoucher < voucher.uniqvouchervoid THEN 'V'
                     ELSE 'VV'
                   END,
       ReferNumber = CONVERT(VARCHAR(20), Space(20-Len(voucher.refernumber))
                                          + Rtrim(voucher.refernumber)),
       voucherline.uniqglaccount,
       voucher.effectivedate,
       UniqEntity = voucher.uniqentitypayee,
       voucher.accountingmonth,
       EntryDescription = voucher.descriptionof,
       DetailDescription = voucherline.descriptionof,
       voucherline.uniqagency,
       voucherline.uniqbranch,
       voucherline.uniqdepartment,
       voucherline.uniqprofitcenter,
       Reversed = '',
       CreditAmount = CASE
                        WHEN voucherline.amount < 0 THEN (
                        voucherline.amount * -1 )
                        ELSE 0
                      END,
       DebitAmount = CASE
                       WHEN voucherline.amount >= 0 THEN voucherline.amount
                       ELSE 0
                     END,
       CheckNumber = NULL,
       voucher.inserteddate,
       voucher.insertedbycode,
       voucherline.glschedulecode,
       VoidNumber = v.refernumber,
       LineType = CASE
                    WHEN voucherline.linenumber = 1 THEN 'F'
                    ELSE 'D'
                  END,
       DetailApplyTo = 'Detail',
       voucherline.applytocode,
       UniqBankAccount = voucher.uniqglaccountbankaccount,
       PaymentGroupNumber = 0,
       LineNumber = voucherline.linenumber,
       voucher.uniqrecurringentry,
       voucherline.uniqpolicyreserve,
       voucherline.uniqlinereserve,
       voucherline.uniqentityreserve,
       voucherline.uniqallocationmethod,
       voucherline.uniqallocationstructuregrouping,
       voucher.payee,
       voucher.routing,
       voucher.comments
FROM   voucher
       INNER JOIN voucherline
               ON voucher.uniqvoucher = voucherline.uniqvoucher
       INNER JOIN voucher v
               ON voucher.uniqvouchervoid = v.uniqvoucher
WHERE  voucherline.uniqvoucherline > -1
       AND voucher.uniqvoucher IN (SELECT ##tmpvoucher.uniqvoucher
                                   FROM   ##tmpvoucher)
       AND NOT ( voucher.accountingmonth = v.accountingmonth );

INSERT INTO ##tmpitems
SELECT ApplyRowID = -1,
       UniqReceipt = -1,
       UniqReceiptLine = -1,
       UniqDisbursement = -1,
       UniqDisbursementLine = -1,
       UniqJournalEntry = -1,
       UniqJournalEntryLine = -1,
       UniqVoucher = -1,
       UniqVoucherLine = -1,
       glaccountbalancelink.uniqglaccountbalancelink,
       EntryType = 'RBLD',
       ReferNumber = NULL,
       glaccountbalance.uniqglaccount,
       EffectiveDate = glaccountbalancelink.posteddate,
       UniqEntity = -1,
       glaccountbalance.accountingmonth,
       EntryDescription = 'Rebuilt General Ledger Account Balance',
       DetailDescription = 'Rebuilt General Ledger Account Balance',
       glaccountbalance.uniqagency,
       glaccountbalance.uniqbranch,
       glaccountbalance.uniqdepartment,
       glaccountbalance.uniqprofitcenter,
       Reversed = '',
       CreditAmount = CASE
                        WHEN glaccountbalance.amount < 0 THEN (
                        glaccountbalance.amount * -1 )
                        ELSE 0
                      END,
       DebitAmount = CASE
                       WHEN glaccountbalance.amount >= 0 THEN
                       glaccountbalance.amount
                       ELSE 0
                     END,
       CheckNumber = NULL,
       inserteddate = glaccountbalancelink.posteddate,
       insertedbycode = glaccountbalancelink.postedbycode,
       GLScheduleCode = NULL,
       VoidNumber = NULL,
       LineType = 'F',
       DetailApplyTo = 'Detail',
       ApplyToCode = NULL,
       UniqBankAccount = -1,
       PaymentGroupNumber = 0,
       LineNumber = 1,
       UniqRecurringEntry = -1,
       UniqPolicyReserve = -1,
       UniqLineReserve = -1,
       UniqEntityReserve = -1,
       UniqAllocationMethod = -1,
       UniqAllocationStructureGrouping = -1,
       Payee = NULL,
       Routing = NULL,
       Comments = NULL
FROM   glaccountbalance
       INNER JOIN glaccountbalancelink
               ON glaccountbalancelink.uniqglaccountbalance =
                             glaccountbalance.uniqglaccountbalance
                  AND glaccountbalancelink.isfromrebuild > 0
WHERE  glaccountbalance.uniqglaccountbalance > -1
       AND (( CONVERT(DATE, glaccountbalancelink.PostedDate) = CONVERT(DATE, GETDATE() - xxx_replace_this_xxx) ));

SELECT ItemRowID = Cast(##tmpitems.itemrowid AS INT),
       ##tmpitems.applyrowid,
       ##tmpitems.uniqreceipt,
       ##tmpitems.uniqreceiptline,
       ##tmpitems.uniqdisbursement,
       ##tmpitems.uniqdisbursementline,
       ##tmpitems.uniqjournalentry,
       ##tmpitems.uniqjournalentryline,
       ##tmpitems.uniqvoucher,
       ##tmpitems.uniqvoucherline,
       ##tmpitems.uniqglaccountbalancelink,
       ##tmpitems.entrytype,
       ##tmpitems.refernumber,
       ##tmpitems.uniqglaccount,
       ##tmpitems.effectivedate,
       ##tmpitems.uniqentity,
       ##tmpitems.accountingmonth,
       ##tmpitems.entrydescription,
       ##tmpitems.detaildescription,
       ##tmpitems.uniqagency,
       ##tmpitems.uniqbranch,
       ##tmpitems.uniqdepartment,
       ##tmpitems.uniqprofitcenter,
       ##tmpitems.reversed,
       ##tmpitems.creditamount,
       ##tmpitems.debitamount,
       ##tmpitems.checknumber,
       ##tmpitems.inserteddate,
       ##tmpitems.insertedbycode,
       ##tmpitems.glschedulecode,
       ##tmpitems.voidnumber,
       ##tmpitems.linetype,
       ##tmpitems.detailapplyto,
       ##tmpitems.applytocode,
       ##tmpitems.uniqbankaccount,
       ##tmpitems.paymentgroupnumber,
       ##tmpitems.linenumber,
       ##tmpitems.uniqrecurringentry,
       ##tmpitems.uniqpolicyreserve,
       ##tmpitems.uniqlinereserve,
       ##tmpitems.uniqentityreserve,
       ##tmpitems.uniqallocationmethod,
       ##tmpitems.uniqallocationstructuregrouping,
       ##tmpitems.payee,
       ##tmpitems.routing,
       ##tmpitems.comments
INTO   ##tmpbase
FROM   ##tmpitems;

CREATE INDEX idx_tmpbase_item637092544039686440
  ON ##tmpbase (itemrowid, applyrowid);

CREATE INDEX idx_tmpbase_glabl637092544039686440
  ON ##tmpbase (uniqglaccountbalancelink, itemrowid);

SELECT ##tmpitems.itemrowid,
       CASE
         WHEN Len(glaccount.subaccount) > 0 THEN Rtrim(glaccount.account) + '-'
                                                 + glaccount.subaccount
         ELSE glaccount.account
       END                           AS [GeneralLedgerRegister.GLAccount],
       agency.agencycode             AS [GeneralLedgerRegister.AgencyCode],
       branch.branchcode             AS [GeneralLedgerRegister.BranchCode],
       department.departmentcode     AS [GeneralLedgerRegister.DepartmentCode],
       profitcenter.profitcentercode AS [GeneralLedgerRegister.ProfitCenterCode]
       ,
       entity.lookupcode             AS
       [GeneralLedgerRegister.PayeeCode],
       ##tmpitems.glschedulecode      AS [GeneralLedgerRegister.ScheduleCode],
       CASE
         WHEN ##tmpitems.entrytype = 'DV' THEN 'Voided Disbursement'
         WHEN ##tmpitems.entrytype = 'D' THEN 'Disbursement'
         WHEN ##tmpitems.entrytype = 'JV' THEN 'Voided Journal Entry'
         WHEN ##tmpitems.entrytype = 'J' THEN 'Journal Entry'
         WHEN ##tmpitems.entrytype = 'MJEV' THEN 'Voided Month-End Entry'
         WHEN ##tmpitems.entrytype = 'MJE' THEN 'Month-End Entry'
         WHEN ##tmpitems.entrytype = 'O' THEN 'Opening Balance'
         WHEN ##tmpitems.entrytype = 'RBLD' THEN
         'Rebuilt General Ledger Account Balance'
         WHEN ##tmpitems.entrytype = 'RV' THEN 'Voided Receipt'
         WHEN ##tmpitems.entrytype = 'R' THEN 'Receipt'
         WHEN ##tmpitems.entrytype = 'VV' THEN 'Voided Voucher'
         WHEN ##tmpitems.entrytype = 'V' THEN 'Voucher'
         WHEN ##tmpitems.entrytype = 'YJE' THEN 'Year-End Entry'
         ELSE ''
       END                           AS
       [GeneralLedgerRegister.GLEntryTypeDescription]
INTO   ##tmpfullnfo
FROM   ##tmpitems
       INNER JOIN glaccount
               ON ##tmpitems.uniqglaccount = glaccount.uniqglaccount
       INNER JOIN agency
               ON ##tmpitems.uniqagency = agency.uniqagency
       INNER JOIN entity
               ON ##tmpitems.uniqentity = entity.uniqentity
       INNER JOIN branch
               ON ##tmpitems.uniqbranch = branch.uniqbranch
       INNER JOIN department
               ON ##tmpitems.uniqdepartment = department.uniqdepartment
       INNER JOIN profitcenter
               ON ##tmpitems.uniqprofitcenter = profitcenter.uniqprofitcenter;

SELECT TypeRefer = ##tmpbase.entrytype + ' '
                   + Isnull(CONVERT(VARCHAR(20),
                   Space(20-Len(Cast(##tmpbase.refernumber
                          AS VARCHAR)))+Rtrim(Cast(##tmpbase.refernumber AS
                   VARCHAR))),
                          ''),
       ##tmpbase.paymentgroupnumber,
       LineNum = ##tmpbase.linenumber,
       Cast(##tmpbase.uniqagency AS VARCHAR) + ','
       + Cast(##tmpbase.uniqbranch AS VARCHAR) + ','
       + Cast(##tmpbase.uniqdepartment AS VARCHAR)
       + ','
       + Cast(##tmpbase.uniqprofitcenter AS VARCHAR) AS SecurityField,
       ##tmpbase.entrytype                           AS
       [GeneralLedgerRegister.GLEntryType],
       ##tmpbase.refernumber                         AS
       [GeneralLedgerRegister.ReferNumber],
       ##tmpbase.reversed                            AS
       [GeneralLedgerRegister.ReversedFlag],
       ##tmpbase.effectivedate                       AS
       [GeneralLedgerRegister.EffectiveDate],
       ##tmpbase.accountingmonth                     AS
       [GeneralLedgerRegister.AccountingMonth],
       ##tmpbase.checknumber                         AS
       [GeneralLedgerRegister.CheckNumberPayID],
       [generalledgerregister.glaccount],
       [generalledgerregister.agencycode],
       [generalledgerregister.branchcode],
       [generalledgerregister.departmentcode],
       [generalledgerregister.profitcentercode],
       [generalledgerregister.payeecode],
       ##tmpbase.voidnumber                          AS
       [GeneralLedgerRegister.VoidNumber],
       [generalledgerregister.schedulecode],
       ##tmpbase.detaildescription                   AS
       [GeneralLedgerRegister.DetailDescription],
       ##tmpbase.debitamount                         AS
       [GeneralLedgerRegister.DebitAmount],
       ##tmpbase.creditamount                        AS
       [GeneralLedgerRegister.CreditAmount],
       [generalledgerregister.glentrytypedescription]
FROM   ##tmpbase
       INNER JOIN ##tmpfullnfo
               ON ##tmpbase.itemrowid = ##tmpfullnfo.itemrowid
ORDER  BY [generalledgerregister.glentrytype],
          [generalledgerregister.refernumber],
          typerefer,
          ##tmpbase.paymentgroupnumber,
          linenum,
          ##tmpbase.applyrowid;

DROP TABLE ##tmpitems;

DROP TABLE ##tmpreceipts;

DROP TABLE ##tmpdisb;

DROP TABLE ##tmpje;

DROP TABLE ##tmpagency;

DROP TABLE ##tmpvoucher;

DROP TABLE ##tmpbase;

DROP TABLE ##tmpfullnfo;
