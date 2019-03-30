CREATE OR REPLACE FUNCTION "public"."load_csv_file"("target_table" text, "csv_path" text, "col_count" int4)
  RETURNS "pg_catalog"."void" AS $BODY$

declare

iter integer;   -- dummy integer zum Hochzählen der Spalten
col text;       -- variable mit dem Spaltennamen bei jedem Durchlauf
col_first text; -- erster Spaltenname, in der Regel der der Eintrag oben links in einer CSV Datei oder in einem Spreadsheet

begin
    set schema 'public';

    DROP TABLE IF EXISTS temp_table;  -- eventuell vorhandene temporäre Tabelle entfernen
    
    create table temp_table ();       -- Temporäre Tabelle erstellen

    -- hinzufügwn von ausreichenden Spaltennamen zur temporären Tabelle
    for iter in 1..col_count
    loop
        execute format('alter table temp_table add column col_%s text;', iter);
    end loop;
		
		--*************************************************************
		BEGIN
      -- Hinzufügen der Daten aus der CSV Dqatei
			execute format('copy temp_table from %L with delimiter '','' quote ''"'' csv ', csv_path);
    EXCEPTION 
    WHEN others THEN    
        RAISE INFO 'No ,';
    END;
		--------------------------------------------------------------
		BEGIN
      -- Hinzufügen der Daten aus der CSV Dqatei
			execute format('copy temp_table from %L with delimiter ''|'' quote ''"'' csv ', csv_path);
    EXCEPTION 
    WHEN others THEN    
        RAISE INFO 'No |';
    END;
		--------------------------------------------------------------
		BEGIN
      -- Hinzufügen der Daten aus der CSV Dqatei
			execute format('copy temp_table from %L with delimiter '';'' quote ''"'' csv ', csv_path);
    EXCEPTION 
    WHEN others THEN    
        RAISE INFO 'No ;';
    END;
		--------------------------------------------------------------
		BEGIN
      -- Hinzufügen der Daten aus der CSV Dqatei
			execute format('copy temp_table from %L with delimiter '''''''' quote ''"'' csv ', csv_path);
    EXCEPTION 
    WHEN others THEN    
        RAISE INFO 'No ''';
    END;
		--------------------------------------------------------------
		BEGIN
      -- Hinzufügen der Daten aus der CSV Dqatei
			execute format('copy temp_table from %L with delimiter ''='' quote ''"'' csv ', csv_path);
    EXCEPTION 
    WHEN others THEN    
        RAISE INFO 'No =';
    END;
		--------------------------------------------------------------
		BEGIN
      -- Hinzufügen der Daten aus der CSV Dqatei
			execute format('copy temp_table from %L with delimiter ''"'' quote '','' csv ', csv_path);
    EXCEPTION 
    WHEN others THEN    
        RAISE INFO 'No "';
    END;
		--*************************************************************
    iter := 1;
    col_first := (select col_1 from temp_table limit 1);


    -- Aktualisieren der temporären Spaltenname (Rename), basierend auf der ersten Zeile der CSV Datei
    for col in execute format('select replace(unnest(string_to_array(trim(temp_table::text, ''()''), '','')),'':'',''_'') from temp_table where col_1 = %L', col_first)
    loop
        execute format('alter table temp_table rename column col_%s to %s', iter, col);
        iter := iter + 1;
    end loop;

    -- Entfernen der Spaltenzeile
    execute format('delete from temp_table where %s = %L', col_first, col_first);

    -- eventuell vorhandene Zieltabelle entfernen
    execute format('DROP TABLE IF EXISTS %I', target_table);
     --  Umbenennen der temporären Tabelle in Zieltabelle
    if length(target_table) > 0 then
        execute format('alter table temp_table rename to %I', target_table);
    end if;
		
end;

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100