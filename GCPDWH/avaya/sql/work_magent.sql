SELECT 
--TO_CHAR(ROW_DATE)  AS ROW_DATE,
--TO_CHAR(ACD)  AS ACD,
--TO_CHAR(SPLIT)  AS SPLIT,
--TO_CHAR(EXTENSION)  AS EXTENSION,
--TO_CHAR(LOGID)  AS LOGID,
--TO_CHAR(LOC_ID)  AS LOC_ID,
--TO_CHAR(RSV_LEVEL)  AS RSV_LEVEL,
--TO_CHAR(I_STAFFTIME)  AS I_STAFFTIME,
--TO_CHAR(TI_STAFFTIME)  AS TI_STAFFTIME,
--TO_CHAR(I_AVAILTIME)  AS I_AVAILTIME,
--TO_CHAR(TI_AVAILTIME)  AS TI_AVAILTIME,
--TO_CHAR(I_ACDTIME)  AS I_ACDTIME,
--TO_CHAR(I_ACWTIME)  AS I_ACWTIME,
--TO_CHAR(I_ACWOUTTIME)  AS I_ACWOUTTIME,
--TO_CHAR(I_ACWINTIME)  AS I_ACWINTIME,
--TO_CHAR(TI_AUXTIME)  AS TI_AUXTIME,
--TO_CHAR(I_AUXOUTTIME)  AS I_AUXOUTTIME,
--TO_CHAR(I_AUXINTIME)  AS I_AUXINTIME,
--TO_CHAR(I_OTHERTIME)  AS I_OTHERTIME,
--TO_CHAR(ACWINCALLS)  AS ACWINCALLS,
--TO_CHAR(ACWINTIME)  AS ACWINTIME,
--TO_CHAR(AUXINCALLS)  AS AUXINCALLS,
--TO_CHAR(AUXINTIME)  AS AUXINTIME,
--TO_CHAR(ACWOUTCALLS)  AS ACWOUTCALLS,
--TO_CHAR(ACWOUTTIME)  AS ACWOUTTIME,
--TO_CHAR(ACWOUTOFFCALLS)  AS ACWOUTOFFCALLS,
--TO_CHAR(ACWOUTOFFTIME)  AS ACWOUTOFFTIME,
--TO_CHAR(ACWOUTADJCALLS)  AS ACWOUTADJCALLS,
--TO_CHAR(AUXOUTCALLS)  AS AUXOUTCALLS,
--TO_CHAR(AUXOUTTIME)  AS AUXOUTTIME,
--TO_CHAR(AUXOUTOFFCALLS)  AS AUXOUTOFFCALLS,
--TO_CHAR(AUXOUTOFFTIME)  AS AUXOUTOFFTIME,
--TO_CHAR(AUXOUTADJCALLS)  AS AUXOUTADJCALLS,
--TO_CHAR(EVENT1)  AS EVENT1,
--TO_CHAR(EVENT2)  AS EVENT2,
--TO_CHAR(EVENT3)  AS EVENT3,
--TO_CHAR(EVENT4)  AS EVENT4,
--TO_CHAR(EVENT5)  AS EVENT5,
--TO_CHAR(EVENT6)  AS EVENT6,
--TO_CHAR(EVENT7)  AS EVENT7,
--TO_CHAR(EVENT8)  AS EVENT8,
--TO_CHAR(EVENT9)  AS EVENT9,
--TO_CHAR(ASSISTS)  AS ASSISTS,
--TO_CHAR(ACDCALLS)  AS ACDCALLS,
--TO_CHAR(ACDTIME)  AS ACDTIME,
--TO_CHAR(ACWTIME)  AS ACWTIME,
--TO_CHAR(O_ACDCALLS)  AS O_ACDCALLS,
--TO_CHAR(O_ACDTIME)  AS O_ACDTIME,
--TO_CHAR(O_ACWTIME)  AS O_ACWTIME,
--TO_CHAR(DA_ACDCALLS)  AS DA_ACDCALLS,
--TO_CHAR(DA_ANSTIME)  AS DA_ANSTIME,
--TO_CHAR(DA_ABNCALLS)  AS DA_ABNCALLS,
--TO_CHAR(DA_ABNTIME)  AS DA_ABNTIME,
--TO_CHAR(HOLDCALLS)  AS HOLDCALLS,
--TO_CHAR(HOLDTIME)  AS HOLDTIME,
--TO_CHAR(HOLDABNCALLS)  AS HOLDABNCALLS,
--TO_CHAR(TRANSFERRED)  AS TRANSFERRED,
--TO_CHAR(CONFERENCE)  AS CONFERENCE,
--TO_CHAR(ABNCALLS)  AS ABNCALLS,
--TO_CHAR(ABNTIME)  AS ABNTIME,
--TO_CHAR(I_RINGTIME)  AS I_RINGTIME,
--TO_CHAR(I_DA_ACDTIME)  AS I_DA_ACDTIME,
--TO_CHAR(I_DA_ACWTIME)  AS I_DA_ACWTIME,
--TO_CHAR(DA_ACDTIME)  AS DA_ACDTIME,
--TO_CHAR(DA_ACWTIME)  AS DA_ACWTIME,
--TO_CHAR(DA_OTHERCALLS)  AS DA_OTHERCALLS,
--TO_CHAR(DA_OTHERTIME)  AS DA_OTHERTIME,
--TO_CHAR(RINGCALLS)  AS RINGCALLS,
--TO_CHAR(RINGTIME)  AS RINGTIME,
--TO_CHAR(ANSRINGTIME)  AS ANSRINGTIME,
--TO_CHAR(TI_OTHERTIME)  AS TI_OTHERTIME,
--TO_CHAR(DA_ACWINCALLS)  AS DA_ACWINCALLS,
--TO_CHAR(DA_ACWINTIME)  AS DA_ACWINTIME,
--TO_CHAR(DA_ACWOCALLS)  AS DA_ACWOCALLS,
--TO_CHAR(DA_ACWOTIME)  AS DA_ACWOTIME,
--TO_CHAR(DA_ACWOADJCALLS)  AS DA_ACWOADJCALLS,
--TO_CHAR(DA_ACWOOFFCALLS)  AS DA_ACWOOFFCALLS,
--TO_CHAR(DA_ACWOOFFTIME)  AS DA_ACWOOFFTIME,
--TO_CHAR(NOANSREDIR)  AS NOANSREDIR,
--TO_CHAR(INCOMPLETE)  AS INCOMPLETE,
--TO_CHAR(ACDAUXOUTCALLS)  AS ACDAUXOUTCALLS,
--TO_CHAR(I_ACDAUX_OUTTIME)  AS I_ACDAUX_OUTTIME,
--TO_CHAR(I_ACDAUXINTIME)  AS I_ACDAUXINTIME,
--TO_CHAR(I_ACDOTHERTIME)  AS I_ACDOTHERTIME,
--TO_CHAR(PHANTOMABNS)  AS PHANTOMABNS,
--TO_CHAR(I_AUXTIME)  AS I_AUXTIME,
--TO_CHAR(HOLDACDTIME)  AS HOLDACDTIME,
--TO_CHAR(DA_RELEASE)  AS DA_RELEASE,
--TO_CHAR(ACD_RELEASE)  AS ACD_RELEASE,
--TO_CHAR(TI_AUXTIME0)  AS TI_AUXTIME0,
--TO_CHAR(TI_AUXTIME1)  AS TI_AUXTIME1,
--TO_CHAR(TI_AUXTIME2)  AS TI_AUXTIME2,
--TO_CHAR(TI_AUXTIME3)  AS TI_AUXTIME3,
--TO_CHAR(TI_AUXTIME4)  AS TI_AUXTIME4,
--TO_CHAR(TI_AUXTIME5)  AS TI_AUXTIME5,
--TO_CHAR(TI_AUXTIME6)  AS TI_AUXTIME6,
--TO_CHAR(TI_AUXTIME7)  AS TI_AUXTIME7,
--TO_CHAR(TI_AUXTIME8)  AS TI_AUXTIME8,
--TO_CHAR(TI_AUXTIME9)  AS TI_AUXTIME9,
--TO_CHAR(ACDCALLS_R1)  AS ACDCALLS_R1,
--TO_CHAR(ACDCALLS_R2)  AS ACDCALLS_R2,
--TO_CHAR(I_OTHERSTBYTIME)  AS I_OTHERSTBYTIME,
--TO_CHAR(I_AUXSTBYTIME)  AS I_AUXSTBYTIME,
--TO_CHAR(INTRNOTIFIES)  AS INTRNOTIFIES,
--TO_CHAR(ACCEPTEDINTRS)  AS ACCEPTEDINTRS,
--TO_CHAR(REJECTEDINTRS)  AS REJECTEDINTRS,
--TO_CHAR(INTRDELIVERIES)  AS INTRDELIVERIES,
--TO_CHAR(TI_AUXTIME10)  AS TI_AUXTIME10,
--TO_CHAR(TI_AUXTIME11)  AS TI_AUXTIME11,
--TO_CHAR(TI_AUXTIME12)  AS TI_AUXTIME12,
--TO_CHAR(TI_AUXTIME13)  AS TI_AUXTIME13,
--TO_CHAR(TI_AUXTIME14)  AS TI_AUXTIME14,
--TO_CHAR(TI_AUXTIME15)  AS TI_AUXTIME15,
--TO_CHAR(TI_AUXTIME16)  AS TI_AUXTIME16,
--TO_CHAR(TI_AUXTIME17)  AS TI_AUXTIME17,
--TO_CHAR(TI_AUXTIME18)  AS TI_AUXTIME18,
--TO_CHAR(TI_AUXTIME19)  AS TI_AUXTIME19,
--TO_CHAR(TI_AUXTIME20)  AS TI_AUXTIME20,
--TO_CHAR(TI_AUXTIME21)  AS TI_AUXTIME21,
--TO_CHAR(TI_AUXTIME22)  AS TI_AUXTIME22,
--TO_CHAR(TI_AUXTIME23)  AS TI_AUXTIME23,
--TO_CHAR(TI_AUXTIME24)  AS TI_AUXTIME24,
--TO_CHAR(TI_AUXTIME25)  AS TI_AUXTIME25,
--TO_CHAR(TI_AUXTIME26)  AS TI_AUXTIME26,
--TO_CHAR(TI_AUXTIME27)  AS TI_AUXTIME27,
--TO_CHAR(TI_AUXTIME28)  AS TI_AUXTIME28,
--TO_CHAR(TI_AUXTIME29)  AS TI_AUXTIME29,
--TO_CHAR(TI_AUXTIME30)  AS TI_AUXTIME30,
--TO_CHAR(TI_AUXTIME31)  AS TI_AUXTIME31,
--TO_CHAR(TI_AUXTIME32)  AS TI_AUXTIME32,
--TO_CHAR(TI_AUXTIME33)  AS TI_AUXTIME33,
--TO_CHAR(TI_AUXTIME34)  AS TI_AUXTIME34,
--TO_CHAR(TI_AUXTIME35)  AS TI_AUXTIME35,
--TO_CHAR(TI_AUXTIME36)  AS TI_AUXTIME36,
--TO_CHAR(TI_AUXTIME37)  AS TI_AUXTIME37,
--TO_CHAR(TI_AUXTIME38)  AS TI_AUXTIME38,
--TO_CHAR(TI_AUXTIME39)  AS TI_AUXTIME39,
--TO_CHAR(TI_AUXTIME40)  AS TI_AUXTIME40,
--TO_CHAR(TI_AUXTIME41)  AS TI_AUXTIME41,
--TO_CHAR(TI_AUXTIME42)  AS TI_AUXTIME42,
--TO_CHAR(TI_AUXTIME43)  AS TI_AUXTIME43,
--TO_CHAR(TI_AUXTIME44)  AS TI_AUXTIME44,
--TO_CHAR(TI_AUXTIME45)  AS TI_AUXTIME45,
--TO_CHAR(TI_AUXTIME46)  AS TI_AUXTIME46,
--TO_CHAR(TI_AUXTIME47)  AS TI_AUXTIME47,
--TO_CHAR(TI_AUXTIME48)  AS TI_AUXTIME48,
--TO_CHAR(TI_AUXTIME49)  AS TI_AUXTIME49,
--TO_CHAR(TI_AUXTIME50)  AS TI_AUXTIME50,
--TO_CHAR(TI_AUXTIME51)  AS TI_AUXTIME51,
--TO_CHAR(TI_AUXTIME52)  AS TI_AUXTIME52,
--TO_CHAR(TI_AUXTIME53)  AS TI_AUXTIME53,
--TO_CHAR(TI_AUXTIME54)  AS TI_AUXTIME54,
--TO_CHAR(TI_AUXTIME55)  AS TI_AUXTIME55,
--TO_CHAR(TI_AUXTIME56)  AS TI_AUXTIME56,
--TO_CHAR(TI_AUXTIME57)  AS TI_AUXTIME57,
--TO_CHAR(TI_AUXTIME58)  AS TI_AUXTIME58,
--TO_CHAR(TI_AUXTIME59)  AS TI_AUXTIME59,
--TO_CHAR(TI_AUXTIME60)  AS TI_AUXTIME60,
--TO_CHAR(TI_AUXTIME61)  AS TI_AUXTIME61,
--TO_CHAR(TI_AUXTIME62)  AS TI_AUXTIME62,
--TO_CHAR(TI_AUXTIME63)  AS TI_AUXTIME63,
--TO_CHAR(TI_AUXTIME64)  AS TI_AUXTIME64,
--TO_CHAR(TI_AUXTIME65)  AS TI_AUXTIME65,
--TO_CHAR(TI_AUXTIME66)  AS TI_AUXTIME66,
--TO_CHAR(TI_AUXTIME67)  AS TI_AUXTIME67,
--TO_CHAR(TI_AUXTIME68)  AS TI_AUXTIME68,
--TO_CHAR(TI_AUXTIME69)  AS TI_AUXTIME69,
--TO_CHAR(TI_AUXTIME70)  AS TI_AUXTIME70,
--TO_CHAR(TI_AUXTIME71)  AS TI_AUXTIME71,
--TO_CHAR(TI_AUXTIME72)  AS TI_AUXTIME72,
--TO_CHAR(TI_AUXTIME73)  AS TI_AUXTIME73,
--TO_CHAR(TI_AUXTIME74)  AS TI_AUXTIME74,
--TO_CHAR(TI_AUXTIME75)  AS TI_AUXTIME75,
--TO_CHAR(TI_AUXTIME76)  AS TI_AUXTIME76,
--TO_CHAR(TI_AUXTIME77)  AS TI_AUXTIME77,
--TO_CHAR(TI_AUXTIME78)  AS TI_AUXTIME78,
--TO_CHAR(TI_AUXTIME79)  AS TI_AUXTIME79,
--TO_CHAR(TI_AUXTIME80)  AS TI_AUXTIME80,
--TO_CHAR(TI_AUXTIME81)  AS TI_AUXTIME81,
--TO_CHAR(TI_AUXTIME82)  AS TI_AUXTIME82,
--TO_CHAR(TI_AUXTIME83)  AS TI_AUXTIME83,
--TO_CHAR(TI_AUXTIME84)  AS TI_AUXTIME84,
--TO_CHAR(TI_AUXTIME85)  AS TI_AUXTIME85,
--TO_CHAR(TI_AUXTIME86)  AS TI_AUXTIME86,
--TO_CHAR(TI_AUXTIME87)  AS TI_AUXTIME87,
--TO_CHAR(TI_AUXTIME88)  AS TI_AUXTIME88,
--TO_CHAR(TI_AUXTIME89)  AS TI_AUXTIME89,
--TO_CHAR(TI_AUXTIME90)  AS TI_AUXTIME90,
--TO_CHAR(TI_AUXTIME91)  AS TI_AUXTIME91,
--TO_CHAR(TI_AUXTIME92)  AS TI_AUXTIME92,
--TO_CHAR(TI_AUXTIME93)  AS TI_AUXTIME93,
--TO_CHAR(TI_AUXTIME94)  AS TI_AUXTIME94,
--TO_CHAR(TI_AUXTIME95)  AS TI_AUXTIME95,
--TO_CHAR(TI_AUXTIME96)  AS TI_AUXTIME96,
--TO_CHAR(TI_AUXTIME97)  AS TI_AUXTIME97,
--TO_CHAR(TI_AUXTIME98)  AS TI_AUXTIME98,
--TO_CHAR(TI_AUXTIME99)  AS TI_AUXTIME99,
--TO_CHAR(ICRPULLCALLS)  AS ICRPULLCALLS,
--TO_CHAR(ICRPULLTIME)  AS ICRPULLTIME,
--TO_CHAR(DA_ICRPULLCALLS)  AS DA_ICRPULLCALLS,
--TO_CHAR(DA_ICRPULLTIME)  AS DA_ICRPULLTIME,
--TO_CHAR(ATTRIB_ID)  AS ATTRIB_ID
*
FROM root.magent
where row_date = 'v_inputdate'