select 
TO_CHAR(translate(TABLE_NM, chr(10)||chr(11)||chr(13),' ')) as TABLE_NM,
TO_CHAR(translate(LOOK_U_CD, chr(10)||chr(11)||chr(13),' ')) as LOOK_U_CD,
TO_CHAR(translate(LOOK_U_DESC, chr(10)||chr(11)||chr(13),' ')) as LOOK_U_DESC,
TO_CHAR(translate(CODE_ORDER, chr(10)||chr(11)||chr(13),' ')) as CODE_ORDER,
TO_CHAR(translate(CODE_TYPE, chr(10)||chr(11)||chr(13),' ')) as CODE_TYPE,
TO_CHAR(translate(COMMENT_REQ, chr(10)||chr(11)||chr(13),' ')) as COMMENT_REQ
from NCA_CAD.LOOK_UP