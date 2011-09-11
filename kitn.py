import imaplib
from email.parser import HeaderParser

from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
from dateutil import relativedelta
import time

import sqlite3
import hashlib
import re
import sys
import os
import getpass

import xlwt
from operator import itemgetter

import gdata
import gdata.docs
import gdata.docs.service
import gdata.spreadsheet.service

ENABLED_SSL = True

MAILBOXES = [ 'INBOX', '[Gmail]/Sent Mail' ]

FILEPATH_DB = './network.sqlite'

IMAP_HOST     = 'imap.gmail.com'
IMAP_PORT     = 993
IMAP_LOGIN    = ''
IMAP_PASSWORD = ''

GOOGLEDOCS_LOGIN = ''
GOOGLEDOCS_PASSWORD = ''
GOOGLEDOCS_SOURCE = 'kitn v0.1'
GOOGLEDOCS_TITLE = 'Contact Status'

EXCEL_FILEPATH = 'contacts.xls'
 
EMAIL_ADDRESSES = [ IMAP_LOGIN ]


regex_numbers = re.compile('([\d]+)')
regex_email   = re.compile( '[^ <>,;"\']*@[^ <>,;"\']*' )
regex_uids    = re.compile( '\d+ \(UID (?P<uid>\d+)' )



def string_to_timestamp( string ):
    time_ = parse_date( string )
    return int( time.mktime( time_.timetuple() ) )


class GoogleSpreadsheetClient:
    def __init__( self, login, password, source ):
        self.source = source
        self.gd_client = gdata.docs.service.DocsService()
        self.gd_client.ClientLogin( login, password, source=source)

        self.gs_client = gdata.spreadsheet.service.SpreadsheetsService()
        self.gs_client.ClientLogin( login, password, source=source)


    def print_feed( self, feed ):
        """Prints out the contents of a feed to the console."""
        print '\n'
        if not feed.entry:
            print 'No entries in feed.\n'
        for entry in feed.entry:
            print '%s %s %s' % (entry.title.text.encode('UTF-8'), entry.GetDocumentType(), entry.resourceId.text)
        print '\n'


    def find_document( self, title ):
        q = gdata.docs.service.DocumentQuery()
        q['title'] = title
        q['title-exact'] = 'true'
        feed = self.gd_client.Query( q.ToUri() )

        entry = None
        if feed.entry:
            for entry_current in feed.entry:
                entry = entry_current
                break

        return entry


    def get_media_source( self, filepath ):
 
        ext = os.path.splitext( filepath )
        if ext: ext = ext[1][1:].upper()
        if not ext or ext not in gdata.docs.service.SUPPORTED_FILETYPES:
            print 'Error: file extension [%s] not supported' % ext[1:]
            return None

        filetype = gdata.docs.service.SUPPORTED_FILETYPES[ ext ]
        ms = gdata.MediaSource(file_path=filepath, content_type=filetype)
        return ms


    def download_spreadsheet( self, filepath, title ):
        entry = self.find_document( title )
        if not entry:
            print 'Download: document not found'
            return None

        return entry


    def upload_spreadsheet( self, filepath, title ):
        ms = self.get_media_source( filepath )
        if not ms:
            print 'Upload: Error media source'
            return None
       
        entry = self.gd_client.Upload( ms, title )

        if entry:
            print 'Upload: OK'
            return entry
        else:
            print 'Upload: ERROR'
            return None

    def update_spreadsheet( self, filepath, title ):
        entry = self.find_document( title )
        if not entry:
            print 'Update: document does not exist'
            return None

        ms = self.get_media_source( filepath )
        if not ms:
            print 'Update: Error media source'
            return None

        entry_updated = self.gd_client.Put( entry, entry.GetEditMediaLink().href, media_source = ms )


        if entry_updated:
            print 'Upload: OK'
            return entry_updated
        else:
            print 'Upload: ERROR'
            return None





class Email:
    def __init__( self, msg ):
        def prepare_item( item ):
            if item:
                return item.replace("'", "").strip()
            return ''
        self.email_from = prepare_item( msg[ 'From' ] )
        self.email_to   = prepare_item( msg[ 'To' ] )
        self.email_cc   = prepare_item( msg[ 'Cc' ] )
        self.email_bcc  = prepare_item( msg[ 'Bcc' ] )
        self.subject    = prepare_item( msg[ 'Subject' ] )
        self.date       = prepare_item( msg[ 'Date' ] )
        self.md5hash    = self.md5()

        if self.email_cc:
            self.email_to = self.email_to + ', ' + self.email_cc

        if self.email_bcc:
            self.email_to = self.email_to + ', ' + self.email_bcc

    def md5( self ):
        text = self.email_from + self.email_to + self.subject + self.date
        return hashlib.md5( text ).hexdigest() 



class EmailManager:

    def __init__( self ):
        self.db = sqlite3.connect( FILEPATH_DB )


    def list_mailboxes( self ):
     
        M = imaplib.IMAP4_SSL( IMAP_HOST, IMAP_PORT ) 
        M.login( IMAP_LOGIN, IMAP_PASSWORD )

        print 'List of mailboxes:'
        rc, response = M.list()
        for item in response:
            print ' => "%s"' % item.split('"')[ -2 ]

        M.logout()


    def __get_most_recent_email_timestamp( self ):
        cursor = self.db.cursor() 
        def get_date_with_field( cursor, field ):
            date = 0
            cursor.execute( 'select max( %s ) from contact' % field )
            for row in cursor:
                date = int( row[ 0 ] )
            return date

        date_to = get_date_with_field( cursor, 'date_to' )
        date_from = get_date_with_field( cursor, 'date_from' )

        return max( date_to, date_from )

                
    def get_emails_all( self ):
        return self.__get_emails( 'ALL' )


    def get_emails_recent( self ):
        ts = self.__get_most_recent_email_timestamp()

        dt = datetime.fromtimestamp( ts - 3600 * 24 * 2 )
        date_string = '(SINCE "%s")' % datetime.strftime( dt, "%d-%b-%Y" )

        return self.__get_emails( date_string )


    def __get_emails( self, search ):
        global IMAP_HOST, IMAP_PORT, IMAP_LOGIN, IMAP_PASSWORD, ENABLED_SSL

        cursor = self.db.cursor()
         
        print 'Connecting to [%s:%s]...' % ( IMAP_HOST, IMAP_PORT )
        if ENABLED_SSL:
            M = imaplib.IMAP4_SSL( IMAP_HOST, IMAP_PORT ) 
        else:
            M = imaplib.IMAP4( IMAP_HOST, IMAP_PORT ) 

        print 'Identifying account [%s]...' % ( IMAP_LOGIN )
        if not IMAP_PASSWORD:
            IMAP_PASSWORD = getpass.getpass( 'Password: ' )

        M.login( IMAP_LOGIN, IMAP_PASSWORD )

        for mailbox in MAILBOXES:
            M.select( mailbox )
            print 'Reading mailbox [%s]...' % mailbox

            typ, data2 = M.search( None, search )
            count = 0
            inserts = 0

            resp, data_uids = M.fetch( ','.join( data2[ 0 ].split() ), "(UID)")

            def data_to_uids( data_uids ):
                uids = []
                for item in data_uids:
                    match = regex_uids.match( item )
                    uids.append( match.group( 'uid' ) )
                return uids

            uids = data_to_uids( data_uids )

            def split_list( items, cut=1000 ):
                nb_blocks = len( items ) / cut
                for i in range( nb_blocks + 1 ):
                    yield items[ i * cut : ( 1 + i ) * cut ]

            nb_emails = len( uids )
            print ' => Total emails: %d' % nb_emails

            for r in split_list( uids, 1000 ):

                uidl_list = r

                resp, data = M.uid('FETCH', ','.join(map(str,uidl_list)) , '(BODY.PEEK[HEADER.FIELDS (From To Cc Bcc Subject Date)] RFC822.SIZE)')
                for index, msg_encoded in enumerate( data ):

                    if not msg_encoded or msg_encoded == ')': continue
                    msg = HeaderParser().parsestr( msg_encoded[ 1 ] )

                    count += 1
                    email = Email( msg )
                    if self.__is_email_in_db( email.md5hash ):
                        continue

                    cursor.execute(
                    """
                    INSERT INTO email
                        ( email_from, email_to, subject, date, hash )
                    values
                        ( \'%s\', \'%s\', \'%s\', \'%s\', \'%s\' )
                    """ % ( email.email_from,
                            email.email_to,
                            email.subject,
                            email.date,
                            email.md5hash  ) )

                    inserts += 1
                    if inserts % 1000 == 0:
                        db.commit()

                self.db.commit()

            print ' => Total emails read:', count
            print ' => Total new emails:', inserts

        M.close()
        M.logout()


    def __is_email_in_db( self, hash_email ):
        ret = False
        cursor = self.db.cursor() 
        cursor.execute( 'select pkid from email where hash=\'%s\'' % hash_email )
        for row in cursor:
            ret = True
            break

        cursor.close()
        return ret



class Contact:

    def __init__( self, pkid, name, email, group, info, contact_rate, date_from, date_to ):
        def prepare_field( value ):
            if value is not None and value.strip() and value != 'None':
                return value
            return ''

        self.pkid = pkid
        self.name = prepare_field( name )
        self.email = prepare_field( email )
        self.group = prepare_field( group )
        self.info = prepare_field( info )
        self.contact_rate = prepare_field( contact_rate )
        self.date_from = date_from
        self.date_to = date_to


    def __str__( self ):
        if self.pkid is None:
            pkid = 'None'
        else:
            pkid = self.pkid

        if self.name is None:
            name  = 'None'
        else:
            name = self.name

        return 'Contact: ' +  str(pkid) + ' ' + str(name) + ' ' + self.email



class ContactManager():
    def __init__( self ):
        self.db = sqlite3.connect( FILEPATH_DB )
        self.cursor = self.db.cursor() 
         

    def get_contacts_from_db( self ):
        self.cursor.execute( 'select pkid, name, email, group_name, info, contact_rate, date_from, date_to from contact order by pkid' )
        contacts = dict( ( int( row[ 0 ] ), Contact( row[ 0 ], row[ 1 ], row[ 2 ], row[ 3 ], row[ 4 ], row[ 5 ] , row[ 6 ] , row[ 7 ] ) ) for row in self.cursor )
        return contacts


    def save_contacts_to_db( self, contacts ):
        contacts_db = self.get_contacts_from_db()
        has_changed_email = False
        for pkid, contact in contacts.iteritems():
            if contacts_db[ pkid ].email != contact.email:
                has_changed_email = True
            self.cursor.execute( 'update contact set name=\'%s\', email=\'%s\', contact_rate=\'%s\', group_name=\'%s\', info=\'%s\' where pkid=%s' % ( contact.name, contact.email, contact.contact_rate, contact.group, contact.info, contact.pkid ) )

        if has_changed_email:
            print 'The email field has changed for at least one entry: need to recompute the dates'
            self.__mark_all_emails_check( 0 )
            self.check_dates()

        self.db.commit()


    def get_contacts_from_gdata( self ):
        gsc = GoogleSpreadsheetClient( GOOGLEDOCS_LOGIN, GOOGLEDOCS_PASSWORD, GOOGLEDOCS_SOURCE )
        feed = gsc.gs_client.GetSpreadsheetsFeed()
        doc_id = None
        for i, entry in enumerate(feed.entry):
            if 'Contact Status' in entry.title.text:
                doc_id = i
                break
                    
        if doc_id is None:
            return None

        id_parts = feed.entry[ doc_id ].id.text.split('/')
        doc_key = id_parts[ len(id_parts) - 1 ]

        feed_worksheet = gsc.gs_client.GetWorksheetsFeed( doc_key )

        id_parts = feed_worksheet.entry[ 0 ].id.text.split('/')
        worksheet_id = id_parts[ len(id_parts) - 1 ]

        list_feed = gsc.gs_client.GetListFeed( doc_key, worksheet_id )
        
        contacts = {}

        for i, entry in enumerate( list_feed.entry ):
            contact = Contact( entry.custom[ 'id' ].text,
                               entry.custom[ 'name' ].text,
                               entry.custom[ 'emailaddresses' ].text,
                               entry.custom[ 'group' ].text,
                               entry.custom[ 'info' ].text,
                               entry.custom[ 'contactrate' ].text,
                               entry.custom[ 'lastfrom' ].text,
                               entry.custom[ 'lastto' ].text )
            contacts[ int( contact.pkid ) ] = contact

        return contacts


    def __timestamp_to_human_readable( self, timestamp ):
        if not timestamp:
            return 'Never', None

        dt = datetime.fromtimestamp( timestamp )
        rd = relativedelta.relativedelta( datetime.now(), dt )

        duration = []

        if rd.years:
            duration.append( '%d y' % rd.years )

        if rd.months:
            duration.append( '%d m' % round( float(rd.months) + float(rd.days) / 31.0 ) )

        if not rd.years and not rd.months and rd.days:
            weeks = rd.days / 7
            days = rd.days % 7
            if weeks:
                duration.append( '%d w' % weeks )
            if not rd.months and days:
                duration.append( '%d d' % days )

        if not duration:
            duration = ['today']
            
        return ( ', '.join( duration ), rd )


    def __rate_to_seconds( self, contact_rate ):
        seconds = 0
        numbers = [ int( n ) for n in regex_numbers.findall( contact_rate ) ]
        if numbers:
            if 'y' in contact_rate:
                seconds += 3600 * 24 * 365 * numbers[ 0 ]
                if 'm' in contact_rate:
                    seconds += 3600 * 24 * 31 * numbers[ 1 ]
            elif 'm' in contact_rate:
                seconds += 3600 * 24 * 31 * numbers[ 0 ]
        return seconds


    def write_contacts_to_file( self, filepath, contacts ):
        style0 = xlwt.easyxf('font: name Verdana, color-index black, bold on')
        style1 = xlwt.easyxf('font: name Verdana')

        style_urgent = xlwt.easyxf('font: name Verdana, color-index red, bold on')
        style_passed = xlwt.easyxf('font: name Verdana, color-index orange, bold on')
        style_ok = xlwt.easyxf('font: name Verdana, color-index green, bold on')

        wb = xlwt.Workbook()
        ws = wb.add_sheet('Contacts')

        titles = ['ID', 'Name', 'Group', 'Contact rate', 'Last from', 'Last to', 'Info', 'Email addresses' ]

        for index, title in enumerate( titles ):
            ws.write( 0, index, title, style0 )

        pkids_nofrom = [ p for p in contacts.keys() if not contacts[ p ].date_from ]
        pkids_noto   = [ p for p in contacts.keys() if not contacts[ p ].date_to and contacts[ p ].date_from ]
        pkids_rate = [ p for p, seconds in sorted( [ ( k, self.__rate_to_seconds( c.contact_rate ) ) for k, c in contacts.iteritems() if c.contact_rate and c.date_from and c.date_to ], key=itemgetter( 1 ) ) ]
        pkids_nonrate = [ p for p in sorted( [ int(k) for k, c in contacts.iteritems() if not c.contact_rate and c.date_from and c.date_to ] ) ]

        pkids_final = []
        pkids_final.extend( pkids_rate )
        pkids_final.extend( pkids_nonrate )
        pkids_final.extend( pkids_nofrom )
        pkids_final.extend( pkids_noto )

        index = 1
        for order in pkids_final:
            contact = contacts[ order ]

            ( duration_from, rd_from ) = self.__timestamp_to_human_readable( contact.date_from )
            ( duration_to, rd_to ) = self.__timestamp_to_human_readable( contact.date_to )

            style_current = style1
            if contact.contact_rate and contact.date_to:
                style_current = style_ok
                seconds_rate = self.__rate_to_seconds( contact.contact_rate )
                timestamp_now = int( time.time() )
                timestamp_due = contact.date_to + seconds_rate

                if timestamp_due <= timestamp_now:
                    style_current = style_urgent

            ws.write( index, 0, contact.pkid, style1 )
            ws.write( index, 1, contact.name, style1 )
            ws.write( index, 2, contact.group, style1 )
            ws.write( index, 3, contact.contact_rate, style1 )
            ws.write( index, 4, duration_from, style_current )
            ws.write( index, 5, duration_to, style_current )
            ws.write( index, 6, contact.info, style1 )
            ws.write( index, 7, contact.email, style1 )
            index += 1
            
        wb.save( filepath )


    def __is_contact_in_db( self, email_address ):
        ret = False
        cursor = self.db.cursor() 
        cursor.execute( 'select pkid from contact where email LIKE \'%%%s%%\'' % email_address )
        for row in cursor:
            ret = True
            break

        cursor.close()
        return ret

        
    def __update_date_in_email_dict( self, dates, email_address, date ):
        if   email_address not in dates \
          or date > dates[ email_address ]:
            dates[ email_address ] = date


    def __mark_all_emails_check( self, value ):
        cursor = self.db.cursor()
        cursor.execute( 'update email set checked=%d' % value )
        self.db.commit()
        cursor.close()


    def __create_email_map( self ):
        cursor = self.db.cursor() 

        dates_from = {}
        dates_to = {}

        nb_errors = 0
        nb_mails = 0

        limit = 100
        offset = 0
        changed = True
        while changed:

            changed = False
            cursor.execute( 'select pkid, email_from, email_to, subject, date from email where checked=0 limit %d offset %d'  % ( limit, offset ) )
            offset += limit

            for row in cursor:
                changed = True
                try:
                    emails_from = set( [ e.lower().strip() for e in regex_email.findall( row[ 1 ] ) ] )
                    emails_to   = set( [ e.lower().strip() for e in regex_email.findall( row[ 2 ] ) ] )
                    date = string_to_timestamp( row[ 4 ] )

                    if any( e in emails_from for e in EMAIL_ADDRESSES ):
                        for email in emails_to:
                            self.__update_date_in_email_dict( dates_to, email, date )
                    
                    if any( e in emails_to for e in EMAIL_ADDRESSES ):
                        for email in emails_from:
                            self.__update_date_in_email_dict( dates_from, email, date )

                    nb_mails += 1

                except:
                    nb_errors += 1

        cursor.close()

        return dates_from, dates_to


    def __update_dates( self, dates, column ):
        cursor = self.db.cursor()

        for email_address, date in dates.iteritems():
            if not self.__is_contact_in_db( email_address ):
                print 'Adding [%s]' % email_address
                cursor.execute('insert into contact ( email ) values ( \'%s\' )' % ( email_address ) )
            
            cursor.execute('update contact set %(column)s=%(date)d where email LIKE \'%%%(email_address)s%%\' and ( %(column)s is null or %(column)s < %(date)d )'
                % { 'column': column,
                    'date': date,
                    'email_address': email_address } )

        self.db.commit()
        cursor.close()


    def check_dates( self ):

        dates_from, dates_to = self.__create_email_map()
        self.__update_dates( dates_from, 'date_from' )
        self.__update_dates( dates_to, 'date_to' )
        self.__mark_all_emails_check( 1 )


 
    def synchronize( self ):
        gsc = GoogleSpreadsheetClient( GOOGLEDOCS_LOGIN, GOOGLEDOCS_PASSWORD, GOOGLEDOCS_SOURCE )
        print 'Read contacts from Google Docs...'
        contacts_user = self.get_contacts_from_gdata()
        if contacts_user is None:
            print 'No contacts found'
        else:
            print 'Save contacts to database...'
            self.save_contacts_to_db( contacts_user )

        print 'Loading contacts from database...'
        contacts_db = self.get_contacts_from_db()

        self.write_contacts_to_file( EXCEL_FILEPATH, contacts_db )
        status = gsc.update_spreadsheet( EXCEL_FILEPATH, GOOGLEDOCS_TITLE )
        if not status:
            gsc.upload_spreadsheet( EXCEL_FILEPATH, GOOGLEDOCS_TITLE )





if __name__=='__main__':
    if len( sys.argv ) == 1:
        print 'Usage: %s all|recent|mailboxes|contacts|sync|up [db_file]'
        print '            all: download all emails in the account'
        print '         recent: download only the emails not retrieved since last download'
        print '      mailboxes: show the mailboxes in the account'
        print '       contacts: extract contacts from emails and update last to/from dates'
        print '           sync: synchronize google doc spreadsheet with local data'
        print '             up: do download recent, contacts, and sync all at once'
        sys.exit( -1 )

    elif len( sys.argv ) == 3:
        FILEPATH_DB = sys.argv[ 2 ]

    em = EmailManager()
    cm = ContactManager()

    if sys.argv[ 1 ] == 'all':
        print 'Getting emails'
        em.get_emails_all()
    elif sys.argv[ 1 ] == 'recent':
        em.get_emails_recent()
    elif sys.argv[ 1 ] == 'mailboxes':
        em.list_mailboxes()
    elif sys.argv[ 1 ] == 'contacts':
        cm.check_dates()
    elif sys.argv[ 1 ] == 'sync':
        action_synchronize()
    elif sys.argv[ 1 ] == 'up':
        em.get_emails_recent()
        cm.check_dates()
        cm.synchronize()
 
