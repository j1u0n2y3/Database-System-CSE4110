#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include "mysql.h"

#pragma comment(lib, "libmysql.lib")

const char* host = "localhost";
const char* user = "root";
const char* pw = "wnsdud1259!";
const char* db = "my_schema";

char type_query[5000];

int main(void) {

	MYSQL* connection = NULL;
	MYSQL conn;
	MYSQL_RES* sql_result;
	MYSQL_ROW sql_row;
	char query[5000];
	int state = 0;
	FILE* crud_txt;

	crud_txt = fopen("queries.txt", "r");
	if (crud_txt == NULL) {
		printf("File open error.\n");
		return 1;
	}
	
	if (mysql_init(&conn) == NULL)
		printf("mysql_init() error!");

	connection = mysql_real_connect(&conn, host, user, pw, db, 3306, (const char*)NULL, 0);
	if (connection == NULL)
	{
		printf("%d ERROR : %s\n", mysql_errno(&conn), mysql_error(&conn));
		return 1;
	}

	else
	{	//connected
		printf("Connection Succeed\n\n");

		if (mysql_select_db(&conn, db))
		{
			printf("%d ERROR : %s\n", mysql_errno(&conn), mysql_error(&conn));
			return 1;
		}

		printf("Building schemas...\n");
		while (!feof(crud_txt)) {
			fgets(query, 4999, crud_txt);
			//printf("%s", query);
			state = mysql_query(connection, query);
		}
		printf(">> Building schemas complete.\n\n\n");
		
		printf("------- SELECT QUERY TYPES -------\n\n");
		printf("\t1. TYPE 1\n");
		printf("\t2. TYPE 2\n");
		printf("\t3. TYPE 3\n");
		printf("\t4. TYPE 4\n");
		printf("\t5. TYPE 5\n");
		printf("\t0. QUIT\n");
		printf("----------------------------------\n");
		int type;
		int temp;
		int year, month;
		while (1) {
			printf("\n\n>> TYPE : ");
			scanf("%d", &type);
			
			switch (type) {
				case 0:
					return 0;
				case 1:
					printf("\n**Assume truck 1721 starting shipping at 2018-07-10 11:00:00 is destroyed.**\n");
					printf("---- Subtypes in TYPE 1 ----\n");
					printf("\t1. TYPE 1-1\n");
					printf("\t2. TYPE 1-2\n");
					printf("\t3. TYPE 1-3\n");
					printf("\n>> SUBTYPE : ");
					scanf("%d", &type);
					if (type == 1) {
						printf("\n---- SUBTYPE 1-1 ----\n\n**Find all customers(senders) who had a package on the truck at the time of the crash**\n");
						sprintf(type_query, "select distinct ID, name from (select * from trans_by natural join transportation where trans_ID='1721' and start_date='2018-07-10 11:00:00') as trans_info join sender as S on S.package_id=trans_info.package_id join customer using(id)");
						strcpy(query, type_query);
						strcpy(type_query, "");
						state = mysql_query(connection, query);
						printf("\n");
						if (state == 0) {
							sql_result = mysql_store_result(connection);
							while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
								printf("Sender ID : %s, Sender name : %s\n", sql_row[0], sql_row[1]);
							}
						}
					}
					else if (type == 2) {
						printf("\n---- SUBTYPE 1-2 ----\n\n**Find all recipients(receivers) who had a package on the truck at the time of the crash**\n");
						sprintf(type_query, "select distinct ID, name from (select * from trans_by natural join transportation where trans_ID='1721' and start_date='2018-07-10 11:00:00') as trans_info join receiver as R on R.package_id=trans_info.package_id join customer using(id)");
						strcpy(query, type_query);
						strcpy(type_query, "");
						state = mysql_query(connection, query);
						printf("\n");
						if (state == 0) {
							sql_result = mysql_store_result(connection);
							while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
								printf("Receiver ID : %s, Receiver name : %s\n", sql_row[0], sql_row[1]);
							}
						}
					}
					else if (type == 3) {
						printf("\n---- SUBTYPE 1-3 ----\n\n**Fine the last successful delivery by that truck prior to the crash**\n");
						sprintf(type_query, "select ID, name, package_ID from trans_by join (select trans_ID, max(start_date) as max from trans_by natural join transportation where trans_ID='1721' and start_date<'2018-07-10 11:00:00') as max_info on max_info.max=trans_by.start_date join sender as s using (package_id) join customer as c using (id)");
						strcpy(query, type_query);
						strcpy(type_query, "");
						state = mysql_query(connection, query);
						printf("\n");
						if (state == 0) {
							sql_result = mysql_store_result(connection);
							while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
								printf("Package ID : %s, Sender ID : %s, Sender name : %s\n", sql_row[0], sql_row[1], sql_row[2]);
							}
						}
					}
					else {
						printf("Invalid Subtype.\n");
					}
					break;
				case 2:
					printf("\n---- TYPE 2 ----\n\n**Find the customer who has shipped the most packages in certain year**\nWhich Year? : ");
					scanf("%d", &temp);
					sprintf(type_query, "select ID, name from customer natural join (select ID, count(package_ID) as counting from sender natural join (select package_id, min(start_date) as package_start_date from trans_by group by package_id having (\'%d-01-01 00:00:00\'<=package_start_date and \'%d-01-01 00:00:00\'>package_start_date)) as package_info group by ID order by counting asc) as year_info", temp, temp + 1);
					strcpy(query, type_query);
					strcpy(type_query, "");
					state = mysql_query(connection, query);
					if (state == 0) {
						sql_result = mysql_store_result(connection);
						sql_row = mysql_fetch_row(sql_result);
						if (sql_row == NULL)	break;
						printf("\nID : %s, name : %s\n", sql_row[0], sql_row[1]);
					}
					break;
				case 3:
					printf("\n---- TYPE 3 ----\n\n**Find the customer who has spent the most money on shipping in certain year**\nWhich Year? : ");
					scanf("%d", &temp);
					sprintf(type_query, "select ID, name, package_cost from customer natural join (select ID, sum((3000+500*weight+500*48*(1/timeliness))) as package_cost from package natural join(select ID, package_ID from sender natural join (select package_id, min(start_date) as package_start_date from trans_by group by package_id having (\'%d-01-01 00:00:00\'<=package_start_date and \'%d-01-01 00:00:00\'>package_start_date)) as package_info) as year_info group by ID) as info order by package_cost desc", temp, temp + 1);
					strcpy(query, type_query);
					strcpy(type_query, "");
					state = mysql_query(connection, query);
					if (state == 0) {
						sql_result = mysql_store_result(connection);
						sql_row = mysql_fetch_row(sql_result);
						if (sql_row == NULL)	break;
						float _money = atof(sql_row[2]);
						int money = (int)_money;
						printf("\nID : %s, name : %s, money spent : %d\n", sql_row[0], sql_row[1], money);
					}
					break;
				case 4:
					printf("\n---- TYPE 4 ----\n\n**Find those packages that were not delivered whthin the promised time in certain year**\nWhich Year? : ");
					scanf("%d", &temp);
					sprintf(type_query, "select package_ID, timediff(duration,timeliness) as latency from(select package_ID, timediff(end,start) as duration, sec_to_time(3600*timeliness) as timeliness from (select package_ID, min(start_date) as start, max(end_date) as end from trans_by natural join transportation group by package_ID having (\'%d-01-01 00:00:00\'<=start and \'%d-01-01 00:00:00\'>start)) as package_info natural join package) as info where duration>timeliness", temp, temp + 1);
					strcpy(query, type_query);
					strcpy(type_query, "");
					printf("\n");
					state = mysql_query(connection, query);
					if (state == 0) {
						sql_result = mysql_store_result(connection);
						while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
							printf("package_ID : %s, latency : %s\n", sql_row[0], sql_row[1]);
						}
					}
					break;
				case 5:
					printf("\n---- TYPE 5 ----\n\n**Generate the bill for each customer for the certain month**\nWhich Year and Month? Input : <Year> <Month> : ");
					scanf("%d %d", &year, &month);
					if (month == 12) {
						sprintf(type_query, "select S.id as sender_id, C1.name as sender_name, role, R.id as receiver_id, C2.name as receiver_name, package_id, paid, weight, timeliness, cast(3000+500*weight+500*48*(1/timeliness) as signed) as package_cost, (cast(3000+500*weight+500*48*(1/timeliness) as signed)-paid) as owed, start from package natural join (select package_ID, min(start_date) as start, max(end_date) as end from trans_by natural join transportation group by package_ID having ('%d-%d-01 00:00:00'<=start and '%d-%d-01 00:00:00'>start)) as package_info join sender as S using (package_ID) join receiver as R using (package_ID)  join customer as C1 on C1.ID=S.ID join customer as C2 on C2.ID=R.ID", year, month, year+1, 1);
					}
					else {
						sprintf(type_query, "select S.id as sender_id, C1.name as sender_name, role, R.id as receiver_id, C2.name as receiver_name, package_id, paid, weight, timeliness, cast(3000+500*weight+500*48*(1/timeliness) as signed) as package_cost, (cast(3000+500*weight+500*48*(1/timeliness) as signed)-paid) as owed, start from package natural join (select package_ID, min(start_date) as start, max(end_date) as end from trans_by natural join transportation group by package_ID having ('%d-%d-01 00:00:00'<=start and '%d-%d-01 00:00:00'>start)) as package_info join sender as S using (package_ID) join receiver as R using (package_ID)  join customer as C1 on C1.ID=S.ID join customer as C2 on C2.ID=R.ID", year, month, year, month + 1);
					}
					strcpy(query, type_query);
					strcpy(type_query, "");
					state = mysql_query(connection, query);
					if (state == 0)
					{
						sql_result = mysql_store_result(connection);
						int i = 1;
						while ((sql_row = mysql_fetch_row(sql_result)) != NULL)
						{
							printf("\n---------------------------------\n");
							printf("|\t\t\t\t|\n");
							printf("| <Bill %d> %s\t|\n", i++, sql_row[11]);
							printf("|\t\t\t\t|\n");
							printf("|\t\t\t\t|\n");
							printf("| Sender ID/Name : \t\t|\n");
							printf("|\t%s/%s\t\t|\n", sql_row[0], sql_row[1]);
							printf("|\t  |- Role : %s\t|\n",sql_row[2]);
							printf("|\t\t\t\t|\n");
							printf("| Receiver ID/Name : \t\t|\n");
							printf("|\t%s/%s\t\t|\n", sql_row[3], sql_row[4]);
							printf("|\t\t\t\t|\n");
							printf("|\t\t\t\t|\n");
							printf("| package ID : \t\t\t|\n");
							printf("|\t%s\t\t\t|\n", sql_row[5]);
							printf("|\t\t\t\t|\n");
							printf("| weight : %s\t\t\t|\n",sql_row[7]);
							printf("| timeliness : %s\t\t|\n", sql_row[8]);
							printf("|  |-cost : %s - %s = %s\t|\n", sql_row[9], sql_row[6], sql_row[10]);
							printf("|\t\t\t\t|\n");
							printf("|\t\t\t\t|\n");
							printf("|\t  Thank You!   \t\t|\n");
							printf("|\t\t\t\t|\n");
							printf("---------------------------------\n");
						}
						i = 1;
						mysql_free_result(sql_result);
					}
					break;
				default:
					printf("Invalid type.\n");
					break;
			}
		}

		mysql_close(connection);
	}

	return 0;
}
