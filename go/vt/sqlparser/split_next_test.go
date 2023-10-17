/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import (
	"github.com/stretchr/testify/require"
	"io"
	"strings"
	"testing"
)

const functionShowCreateTable = `CREATE OR REPLACE FUNCTION "public"."showcreatetable"("namespace" varchar, "tablename" varchar) RETURNS "pg_catalog"."varchar" AS $BODY$
        declare
        tableScript character varying default '';
        begin
        -- columns
        tableScript:=tableScript || ' CREATE TABLE '|| tablename|| ' ( '|| chr(13)||chr(10) || array_to_string(
        array(
        select ' ' || concat_ws(' ',fieldName, fieldType, fieldLen, indexType, isNullStr, fieldComment ) as column_line
        from (
        select a.attname as fieldName,format_type(a.atttypid,a.atttypmod) as fieldType,(case when atttypmod-4>0 then
        atttypmod-4 else 0 end) as fieldLen,
        (case when (select count(*) from pg_constraint where conrelid = a.attrelid and conkey[1]=attnum and
        contype='p')>0 then 'PRI'
        when (select count(*) from pg_constraint where conrelid = a.attrelid and conkey[1]=attnum and contype='u')>0
        then 'UNI'
        when (select count(*) from pg_constraint where conrelid = a.attrelid and conkey[1]=attnum and contype='f')>0
        then 'FRI'
        else '' end) as indexType,
        (case when a.attnotnull=true then 'not null' else 'null' end) as isNullStr,
        ' comment ' || col_description(a.attrelid,a.attnum) as fieldComment
        from pg_attribute a where attstattarget=-1 and attrelid = (select c.oid from pg_class c,pg_namespace n where
        c.relnamespace=n.oid and n.nspname =namespace and relname =tablename)
        ) as string_columns
        ),','||chr(13)||chr(10)) || ',';
        -- 约束
        tableScript:= tableScript || chr(13)||chr(10) || array_to_string(
        array(
        select concat(' CONSTRAINT ',conname ,c ,u,p,f) from (
        select conname,
        case when contype='c' then ' CHECK('|| ( select findattname(namespace,tablename,'c') ) ||')' end as c ,
        case when contype='u' then ' UNIQUE('|| ( select findattname(namespace,tablename,'u') ) ||')' end as u ,
        case when contype='p' then ' PRIMARY KEY ('|| ( select findattname(namespace,tablename,'p') ) ||')' end as p ,
        case when contype='f' then ' FOREIGN KEY('|| ( select findattname(namespace,tablename,'u') ) ||') REFERENCES '||
        (select p.relname from pg_class p where p.oid=c.confrelid ) || '('|| ( select
        findattname(namespace,tablename,'u') ) ||')' end as f
        from pg_constraint c
        where contype in('u','c','f','p') and conrelid=(
        select oid from pg_class where relname=tablename and relnamespace =(
        select oid from pg_namespace where nspname = namespace
        )
        )
        ) as t
        ) ,',' || chr(13)||chr(10) ) || chr(13)||chr(10) ||' ); ';
        -- indexs
        -- CREATE UNIQUE INDEX pg_language_oid_index ON pg_language USING btree (oid); -- table pg_language
        --
        /** **/
        --- 获取非约束索引 column
        -- CREATE UNIQUE INDEX pg_language_oid_index ON pg_language USING btree (oid); -- table pg_language
        tableScript:= tableScript || chr(13)||chr(10) || chr(13)||chr(10) || array_to_string(
        array(
        select 'CREATE INDEX ' || indexrelname || ' ON ' || tablename || ' USING btree '|| '(' || attname || ');' from (
        SELECT
        i.relname AS indexrelname , x.indkey,
        ( select array_to_string (
        array(
        select a.attname from pg_attribute a where attrelid=c.oid and a.attnum in ( select unnest(x.indkey) )
        )
        ,',' ) )as attname
        FROM pg_class c
        JOIN pg_index x ON c.oid = x.indrelid
        JOIN pg_class i ON i.oid = x.indexrelid
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname=tablename and i.relname not in
        ( select constraint_name from information_schema.key_column_usage where table_name=tablename )
        )as t
        ) ,','|| chr(13)||chr(10));
        -- COMMENT COMMENT ON COLUMN sys_activity.id IS '主键';
        tableScript:= tableScript || chr(13)||chr(10) || chr(13)||chr(10) || array_to_string(
        array(
        SELECT 'COMMENT ON COLUMN' || tablename || '.' || a.attname ||' IS '|| ''''|| d.description ||''''
        FROM pg_class c
        JOIN pg_description d ON c.oid=d.objoid
        JOIN pg_attribute a ON c.oid = a.attrelid
        WHERE c.relname=tablename
        AND a.attnum = d.objsubid),','|| chr(13)||chr(10)) ;
        return tableScript;
        end
        $BODY$ LANGUAGE plpgsql VOLATILE COST 100`

const insertIntoRuleDescSections = `INSERT INTO "public"."rule_desc_sections" VALUES ('AYq8jq9Zzf2mMqOTe-St', 'AYq8jq9Zzf2mMqOTe-Ss', 'default', '<p>Typically, backslashes are seen only as part of escape sequences. Therefore, the use of a backslash outside of a raw string or escape sequence
looks suspiciously like a broken escape sequence.</p>
<p>Characters recognized as escape-able are: <code>abfnrtvox\''"</code></p>
<h2>Noncompliant Code Example</h2>
<pre>
s = "Hello \world."
t = "Nice to \ meet you"
u = "Let''s have \ lunch"
</pre>
<h2>Compliant Solution</h2>
<pre>
s = "Hello world."
t = "Nice to \\ meet you"
u = r"Let''s have \ lunch"  // raw string
</pre>
<h2>Deprecated</h2>
<p>This rule is deprecated, and will eventually be removed.</p>', NULL, NULL);
select 1;`

func Test_SplitNext(t *testing.T) {
	testcases := []struct {
		name    string
		input   string
		output  string
		count   int
		dialect Dialect
	}{
		{
			name:    "mysql select '\\'''",
			input:   "select '\\\\\\'hello''';select 2",
			count:   2,
			dialect: MysqlDialect{},
		},
		{
			name:    "postgres select '\\'''",
			input:   "select '\\''hello''' from dual;select 2",
			count:   2,
			dialect: PostgresDialect{},
		},
		{
			name:   "with blanks",
			input:  "select * from `my-table`; \t; \n; \n\t\t ;select * from `my-table`;",
			output: "select * from `my-table`;select * from `my-table`",
			count:  2,
		},
		{
			name:  "with `my-table`",
			input: "select * from `my-table`",
		},
		{
			name:   "ending with a semicolon",
			input:  "select * from `my-table`;",
			output: "select * from `my-table`",
		},
		{
			name:   "ending with a semicolon and some spaces",
			input:  "select * from `my-table`;   ",
			output: "select * from `my-table`",
		},
		{
			name:   "two statements",
			input:  "select * from `my-table1`; select * from `my-table2`;",
			output: "select * from `my-table1`;select * from `my-table2`",
			count:  2,
		},
		{
			name:   "with a comment",
			input:  "select * from /* comment ; */ `my-table`;",
			output: "select * from `my-table`",
		},
		{
			name:   "string with a semicolon",
			input:  "select * from `my-table` where semi = ';';",
			output: "select * from `my-table` where semi = ';'",
		},
		{
			name:   "with a comment ending with line break",
			input:  "select * from `my-table1`;-- comment;\nselect * from `my-table2`;",
			output: "select * from `my-table1`;select * from `my-table2`",
			count:  2,
		},
		{
			name: "create table `total_data`",
			input: "CREATE TABLE `total_data` (`id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id', " +
				"`region` varchar(32) NOT NULL COMMENT 'region name, like zh; th; kepler'," +
				"`data_size` bigint NOT NULL DEFAULT '0' COMMENT 'data size;'," +
				"`createtime` datetime NOT NULL DEFAULT NOW() COMMENT 'create time;'," +
				"`comment` varchar(100) NOT NULL DEFAULT '' COMMENT 'comment'," +
				"PRIMARY KEY (`id`))",
		},
		{
			name:  "create table \"my-table\"",
			input: "create table \"my-table\" (\"my-id\" bigint(20))",
		},
		{
			name: "create procedure as $$",
			input: `CREATE PROCEDURE insert_data(a varchar(50), b varchar(50))
						LANGUAGE SQL
						AS $$
						/*this is the comment */
						INSERT /* inline comment */ INTO tbl VALUES ('lkjafd''lksjadlf;lk\\jasdf\'lkasdf"asdklf\\');
						-- this is the comment
						INSERT INTO tbl VALUES ('fasf_bkdjlfa');
						$$;
						CREATE TABLE t(a int);`,
			output: `CREATE PROCEDURE insert_data(a varchar(50), b varchar(50)) LANGUAGE SQL AS $$
						/*this is the comment */
						INSERT /* inline comment */ INTO tbl VALUES ('lkjafd''lksjadlf;lk\\jasdf\'lkasdf"asdklf\\');
						-- this is the comment
						INSERT INTO tbl VALUES ('fasf_bkdjlfa');
						$$;` +
				"CREATE TABLE t(a int)",
			count: 2,
		},
		{
			name: "create procedure as $tag_name$",
			input: `CREATE PROCEDURE insert_data(a varchar(50), b varchar(50))
						LANGUAGE SQL
						AS $tag_name$
						/*this is the comment */
						INSERT /* inline comment */ INTO tbl VALUES ('lkjafd''lksjadlf;lkjasdf\'lkasdf"asdklf');
						-- this is the comment
						INSERT INTO tbl VALUES ('fasf_bkdjlfa');
						$tag_name$;
						CREATE TABLE t(a int);`,
			output: `CREATE PROCEDURE insert_data(a varchar(50), b varchar(50)) LANGUAGE SQL AS $tag_name$
						/*this is the comment */
						INSERT /* inline comment */ INTO tbl VALUES ('lkjafd''lksjadlf;lkjasdf\'lkasdf"asdklf');
						-- this is the comment
						INSERT INTO tbl VALUES ('fasf_bkdjlfa');
						$tag_name$;` +
				"CREATE TABLE t(a int)",
			count: 2,
		},
		{
			name:  "create function showcreatetable",
			input: functionShowCreateTable,
			count: 1,
		},
		{
			name:    "insert into rule_desc_sections",
			input:   insertIntoRuleDescSections,
			count:   2,
			dialect: PostgresDialect{},
		},
	}

	for _, tcase := range testcases {
		t.Run(tcase.name, func(t *testing.T) {
			if tcase.output == "" {
				tcase.output = tcase.input
			}
			if tcase.dialect == nil {
				tcase.dialect = MysqlDialect{}
			}
			tokenizer := NewReaderTokenizer(strings.NewReader(tcase.input),
				WithCacheInBuffer(), WithDialect(tcase.dialect))
			var sb strings.Builder
			var i int
			for {
				stmt, err := SplitNext(tokenizer)
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				if sb.Len() > 0 {
					sb.WriteString(";")
				}
				sb.WriteString(strings.TrimSpace(stmt))
				i++
			}
			if tcase.count == 0 {
				tcase.count = 1
			}
			require.Equal(t, tcase.count, i)
			require.Equal(t, tcase.output, sb.String())
		})
	}
}

func Test_SplitNext_WithLongString(t *testing.T) {
	const times = 0x1000
	sb := strings.Builder{}
	sb.WriteString("select '")
	for i := 0; i < times; i++ {
		sb.WriteString("0123456789")
	}
	sb.WriteString("', '\\n")
	for i := 0; i < times; i++ {
		sb.WriteString("0123456789")
	}
	sb.WriteString("' from tbl")
	token := NewReaderTokenizer(strings.NewReader(sb.String()))
	require.NotPanics(t, func() {
		stmt, err := ParseNext(token)
		require.NoError(t, err)
		require.Equal(t, sb.String(), String(stmt))
	})
}

func TestSplitNextIgnoreSpecialComments(t *testing.T) {
	input := `SELECT 1;/*! ALTER TABLE foo DISABLE KEYS */;SELECT 2;`

	tokenizer := NewReaderTokenizer(strings.NewReader(input), WithCacheInBuffer())
	tokenizer.SkipSpecialComments = true
	one, err := SplitNext(tokenizer)
	require.NoError(t, err)
	require.Equal(t, "SELECT 1", one)
	two, err := SplitNext(tokenizer)
	require.NoError(t, err)
	require.Equal(t, "SELECT 2", two)
}

// TestSplitNextEdgeCases tests various SplitNext edge cases.
func TestSplitNextEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{{
		name:  "Trailing ;",
		input: "select 1 from a; update a set b = 2;",
		want:  []string{"select 1 from a", "update a set b = 2"},
	}, {
		name:  "No trailing ;",
		input: "select 1 from a; update a set b = 2",
		want:  []string{"select 1 from a", "update a set b = 2"},
	}, {
		name:  "Trailing whitespace",
		input: "select 1 from a; update a set b = 2    ",
		want:  []string{"select 1 from a", "update a set b = 2"},
	}, {
		name:  "Trailing whitespace and ;",
		input: "select 1 from a; update a set b = 2   ;   ",
		want:  []string{"select 1 from a", "update a set b = 2"},
	}, {
		name:  "Handle SkipToEnd statements",
		input: "set character set utf8; select 1 from a",
		want:  []string{"set character set utf8", "select 1 from a"},
	}, {
		name:  "Semicolin inside a string",
		input: "set character set ';'; select 1 from a",
		want:  []string{"set character set ';'", "select 1 from a"},
	}, {
		name:  "Partial DDL",
		input: "create table a; select 1 from a",
		want:  []string{"create table a", "select 1 from a"},
	}, {
		name:  "Partial DDL",
		input: "create table a ignore me this is garbage; select 1 from a",
		want:  []string{"create table a ignore me this is garbage", "select 1 from a"},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tokens := NewReaderTokenizer(strings.NewReader(test.input), WithCacheInBuffer())

			for _, want := range test.want {
				stmt, err := SplitNext(tokens)
				require.NoError(t, err)
				require.Equal(t, want, strings.TrimSpace(stmt))
			}

			// Read once more and it should be EOF.
			if tree, err := SplitNext(tokens); err != io.EOF {
				t.Errorf("SplitNext(%q) = (%q, %v) want io.EOF", test.input, tree, err)
			}

			// And again, once more should be EOF.
			if tree, err := SplitNext(tokens); err != io.EOF {
				t.Errorf("SplitNext(%q) = (%q, %v) want io.EOF", test.input, tree, err)
			}
		})
	}
}

func BenchmarkSplitToPieces(b *testing.B) {
	const sql = `SELECT DISTINCT ca.l9_convergence_code AS atb2, cu.cust_sub_type AS account_type, cst.description AS account_type_desc, ss.prim_resource_val AS msisdn, ca.ban AS ban_key, To_char(mo.memo_date, 'YYYYMMDD') AS memo_date, cu.l9_identification AS thai_id, ss.subscriber_no AS subs_key, ss.dealer_code AS shop_code, cd.description AS shop_name, mot.short_desc, Regexp_substr(mo.attr1value, '[^ ;]+', 1, 3) staff_id, mo.operator_id AS user_id, mo.memo_system_text, co2.soc_name AS first_socname, co3.soc_name AS previous_socname, co.soc_name AS current_socname, Regexp_substr(mo.attr1value, '[^ ; ]+', 1, 1) NAME, co.soc_description AS current_pp_desc, co3.soc_description AS prev_pp_desc, co.soc_cd AS soc_cd, ( SELECT Sum(br.amount) FROM bl1_rc_rates BR, customer CU, subscriber SS WHERE br.service_receiver_id = ss.subscriber_no AND br.receiver_customer = ss.customer_id AND br.effective_date <= br.expiration_date AND (( ss. sub_status <> 'C' AND ss. sub_status <> 'T' AND br.expiration_date IS NULL) OR ( ss. sub_status = 'C' AND br.expiration_date LIKE ss.effective_date)) AND br.pp_ind = 'Y' AND br.cycle_code = cu.bill_cycle) AS pp_rate, cu.bill_cycle AS cycle_code, To_char(Nvl(ss.l9_tmv_act_date, ss.init_act_date),'YYYYMMDD') AS activated_date, To_char(cd.effective_date, 'YYYYMMDD') AS shop_effective_date, cd.expiration_date AS shop_expired_date, ca.l9_company_code AS company_code FROM service_details S, product CO, csm_pay_channel CPC, account CA, subscriber SS, customer CU, customer_sub_type CST, csm_dealer CD, service_details S2, product CO2, service_details S3, product CO3, memo MO , memo_type MOT, logical_date LO, charge_details CHD WHERE ss.subscriber_no = chd.agreement_no AND cpc.pym_channel_no = chd.target_pcn AND chd.chg_split_type = 'DR' AND chd.expiration_date IS NULL AND s.soc = co.soc_cd AND co.soc_type = 'P' AND s.agreement_no = ss.subscriber_no AND ss.prim_resource_tp = 'C' AND cpc.payment_category = 'POST' AND ca.ban = cpc.ban AND ( ca.l9_company_code = 'RF' OR ca.l9_company_code = 'RM' OR ca.l9_company_code = 'TM') AND ss.customer_id = cu.customer_id AND cu.cust_sub_type = cst.cust_sub_type AND cu.customer_type = cst.customer_type AND ss.dealer_code = cd.dealer AND s2.effective_date= ( SELECT Max(sa1.effective_date) FROM service_details SA1, product o1 WHERE sa1.agreement_no = ss.subscriber_no AND co.soc_cd = sa1.soc AND co.soc_type = 'P' ) AND s2.agreement_no = s.agreement_no AND s2.soc = co2.soc_cd AND co2.soc_type = 'P' AND s2.effective_date = ( SELECT Min(sa1.effective_date) FROM service_details SA1, product o1 WHERE sa1.agreement_no = ss.subscriber_no AND co2.soc_cd = sa1.soc AND co.soc_type = 'P' ) AND s3.agreement_no = s.agreement_no AND s3.soc = co3.soc_cd AND co3.soc_type = 'P' AND s3.effective_date = ( SELECT Max(sa1.effective_date) FROM service_details SA1, product o1 WHERE sa1.agreement_no = ss.subscriber_no AND sa1.effective_date < ( SELECT Max(sa1.effective_date) FROM service_details SA1, product o1 WHERE sa1.agreement_no = ss.subscriber_no AND co3.soc_cd = sa1.soc AND co3.soc_type = 'P' ) AND co3.soc_cd = sa1.soc AND o1.soc_type = 'P' ) AND mo.entity_id = ss.subscriber_no AND mo.entity_type_id = 6 AND mo.memo_type_id = mot.memo_type_id AND Trunc(mo.sys_creation_date) = ( SELECT Trunc(lo.logical_date - 1) FROM lo) AND lo.expiration_date IS NULL AND lo.logical_date_type = 'B' AND lo.expiration_date IS NULL AND ( mot.short_desc = 'BCN' OR mot.short_desc = 'BCNM' );`
	var sb strings.Builder
	for i := 0; i < 8; i++ {
		sb.WriteString(sql)
	}
	testCases := []struct {
		name    string
		handler func(string) ([]string, error)
	}{
		{
			name: "ParseNext",
			handler: func(sql string) ([]string, error) {
				var tree Statement
				var err error
				stmts := make([]string, 0, 16)
				tkn := NewStringTokenizer(sql)
				for {
					tree, err = ParseNext(tkn)
					if err != nil {
						break
					}
					stmts = append(stmts, String(tree))
				}
				return stmts, err
			},
		},
		{
			name: "SplitNext",
			handler: func(sql string) ([]string, error) {
				var stmt string
				var err error
				stmts := make([]string, 0, 16)
				tkn := NewStringTokenizer(sql, WithCacheInBuffer())
				for {
					stmt, err = SplitNext(tkn)
					if err != nil {
						break
					}
					stmts = append(stmts, stmt)
				}
				return stmts, err
			},
		},
		{
			name: "SplitToPieces",
			handler: func(sql string) ([]string, error) {
				return SplitStatementToPieces(sql)
			},
		},
	}
	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := tc.handler(sb.String())
				if err != io.EOF && err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
