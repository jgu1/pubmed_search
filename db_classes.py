import pdb
import os,sqlite3
import esearch_fetch_parse
from datetime import datetime
class DAO(object):
    db = None    

    def insert_one_search_term_into_table_search_terms(self,search_term,result_count):
        sql_template = 'insert into search_terms(search_term,num_papers,last_update) values(?,?,?)'
        #insert
        try:
            cursor = self.db.execute(sql_template,[search_term,result_count,datetime.now()])
        except sqlite3.ProgrammingError:
            cursor = self.db.execute(sql_template,[unicode(search_term),unicode(result_count),datetime.now()])
            self.db.commit()
        #get primary_key
        return cursor.lastrowid

    def insert_one_paper_into_table_papers(self,title,link,authors_str,journal_title,publish_time_str,abstract,keywords_str):
        #insert
        sql_template = 'insert into papers (title,link,authors_str,journal_title,publish_time_str,abstract,keywords_str) values(?,?,?,?,?,?,?)'
        try:
            cursor = self.db.execute(sql_template,[title,link,authors_str,journal_title,publish_time_str,abstract,keywords_str])
       
        except sqlite3.ProgrammingError:
            cursor = self.db.execute(sql_template,[unicode(title),unicode(link),unicode(authors_str),unicode(journal_title),unicode(publish_time_str),unicode(abstract),unicode(keywords_str)])
        except sqlite3.IntegrityError:
            pdb.set_trace()
            a=1            
        self.db.commit()
        #get primary_key
        return cursor.lastrowid

    def insert_one_relation_into_table_term_paper_relation(self,term_id,paper_id):
        sql_template = 'insert into term_paper_relation (term_id, paper_id) values(?,?)'
        self.db.execute(sql_template,[term_id,paper_id])
        self.db.commit()


    def fetch_search_terms(self,disease,genes_included,genes_excluded):
        papers=[]
        count_dict={}
        disease = '+'.join(disease.split())
        for gene in genes_included:
            search_term = 'AND+' + disease + '+' + gene
            if not not genes_excluded:
                search_term += '+NOT+'+ '+'.join(genes_excluded)

            papers_for_curr_search_term = self.fetch_one_search_term(search_term)
            count_dict[gene]=len(papers_for_curr_search_term)
            papers += papers_for_curr_search_term
        return papers,count_dict

    def fetch_one_search_term(self,search_term):
        sql_fetch_all_papers_for_search_term = ('with paper_ids as'
             ' (select paper_id from search_terms as S ,term_paper_relation as T'
             ' where S.search_term="'+ search_term +'" and S.id = T.term_id)'
             ' select title,link,authors_str,journal_title,publish_time_str,abstract,keywords_str from paper_ids, papers'
             ' where paper_ids.paper_id = papers.id;')
        cur = self.db.execute(sql_fetch_all_papers_for_search_term)
        papers = [dict(title=row[0], link=row[1], authors_str=row[2],journal_title=row[3],publish_time_str=row[4],abstract=row[5],keywords_str=row[6],search_term=search_term) for row in cur.fetchall()]
        return papers

    def delete_one_search_term(self,search_term):
        sql_delete_all_papers_for_search_term = ('with paper_ids as'
             ' (select paper_id from search_terms as S ,term_paper_relation as T'
             ' where S.search_term="'+ search_term +'" and S.id = T.term_id)'
             ' delete from papers'
             ' where papers.id in paper_ids;')
        cur = self.db.execute(sql_delete_all_papers_for_search_term)

        sql_delete_all_term_paper_relations_for_search_term = ('with term_ids as'
            ' (select term_id from term_paper_relation as T, search_terms as S'
            ' where T.term_id = S.id and S.search_term="' + search_term + '") '
            ' delete from term_paper_relation'
            ' where term_paper_relation.term_id in term_ids ; ')
        cur = self.db.execute(sql_delete_all_term_paper_relations_for_search_term)

        sql_delete_search_terms_for_search_term = ('delete from search_terms'
            ' where search_terms.search_term="' + search_term + '" ;'
            )
        cur = self.db.execute(sql_delete_search_terms_for_search_term)
        self.db.commit()

    def pop_db(self,disease,genes_included,genes_excluded):
        
        disease='+'.join(disease.split())
     
        for gene in genes_included:
            search_term = 'AND+' + disease + '+' + gene
            if not not genes_excluded:
                search_term += '+NOT+'+ '+'.join(genes_excluded)
            #pdb.set_trace()
            sql_check_if_search_term_has_results_already = ('select last_update from search_terms'
                 ' where search_term="' + search_term +'"')
            cur = self.db.execute(sql_check_if_search_term_has_results_already)
            result = cur.fetchall()
            if len(result) > 0:
                time_delta = datetime.now() - datetime.strptime(result[0][0], "%Y-%m-%d %H:%M:%S.%f")
                if time_delta.seconds > 60*60*4: # re-fetch if the record is older than 4 hours
                    self.delete_one_search_term(search_term)
                    esearch_fetch_parse.Main(self.DATABASE,search_term,gene)
            else:
                esearch_fetch_parse.Main(self.DATABASE,search_term,gene)


    def __init__(self,DATABASE):
        self.DATABASE = DATABASE
        self.db = sqlite3.connect(DATABASE)
