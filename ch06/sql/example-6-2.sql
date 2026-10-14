SELECT e.sequence_number,
       e.data_file.content,
       e.data_file.record_count,
       e.data_file.file_path
FROM lake.crm.customers.entries AS e
WHERE e.sequence_number =
      (SELECT max(sequence_number) FROM lake.crm.customers.entries);
