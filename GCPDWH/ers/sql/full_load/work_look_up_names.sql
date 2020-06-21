select 
TO_CHAR(translate(TABLE_NM, chr(10)||chr(11)||chr(13),' ')) as TABLE_NM,
TO_CHAR(translate(TABLE_DESC, chr(10)||chr(11)||chr(13),' ')) as TABLE_DESC,
TO_CHAR(translate(TABLE_MOD, chr(10)||chr(11)||chr(13),' ')) as TABLE_MOD,
TO_CHAR(LAST_UPDT,'YYYY-MM-DD HH24:MI:SS') as LAST_UPDT,
TO_CHAR(translate(DISPLAY_FLG, chr(10)||chr(11)||chr(13),' ')) as DISPLAY_FLG,
TO_CHAR(translate(REASON_MOD, chr(10)||chr(11)||chr(13),' ')) as REASON_MOD,
TO_CHAR(translate(CATEGORY, chr(10)||chr(11)||chr(13),' ')) as CATEGORY,
TO_CHAR(translate(MAX_CHARS, chr(10)||chr(11)||chr(13),' ')) as MAX_CHARS,
TO_CHAR(translate(COMMENT_REQ, chr(10)||chr(11)||chr(13),' ')) as COMMENT_REQ
from NCA_CAD.LOOK_UP_NAMES