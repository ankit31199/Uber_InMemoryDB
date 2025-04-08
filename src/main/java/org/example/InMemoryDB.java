package org.example;

import java.util.*;
import java.util.stream.Collectors;

public class InMemoryDB {

    private final Map<String, Map<String, ValueWithTTL>> dataStore;
    private final TreeMap<Integer, Map<String, Map<String, ValueWithTTL>>> backupStore;

    private static class ValueWithTTL{

        final String value;
        final int expirationTime;

        ValueWithTTL(String value, int expirationTime)
        {
            this.value=value;
            this.expirationTime=expirationTime;
        }

        boolean isExpired(int currentTime)
        {
            return expirationTime != -1 && expirationTime < currentTime;
        }

    }

    public InMemoryDB()
    {
        this.dataStore= new HashMap<>();
        this.backupStore=new TreeMap<>();
    }

    public void setAt(String key, String field, String value, int timestamp)
    {
        if(key == null || field == null)
            throw new IllegalArgumentException("Key and Field cannot be null");

        dataStore.computeIfAbsent(key, k -> new HashMap<>())
                .put(field, new ValueWithTTL(value, -1));
    }

    public void setWithTTL(String key,String field, String value, int timestamp, int ttl)
    {
        if(key == null || field == null)
            throw new IllegalArgumentException("Invalid Parameter");

        dataStore.computeIfAbsent(key, k -> new HashMap<>())
                .put(field, new ValueWithTTL(value, timestamp+ttl));
    }

    public Optional<String> getAt(String key, String field, int timestamp)
    {
        if(key ==null || field == null )
            return Optional.empty();

        return Optional.ofNullable(dataStore.get(key))
                .map(record -> record.get(field))
                .filter(value -> !value.isExpired(timestamp))
                .map(value -> value.value);

    }

    public boolean deleteAt(String key, String field, int timestamp)
    {
        if(key ==null || field == null )
            return false;

        Map<String, ValueWithTTL> record= dataStore.get(key);
        if(record == null)
            return false;

        ValueWithTTL valueWithTTL=record.get(field);
        if(valueWithTTL == null || valueWithTTL.isExpired(timestamp))
            return false;

        record.remove(field);

        if (record.isEmpty())
            dataStore.remove(key);

        return true;

    }

    public List<String> scanAt(String key, int timestamp)
    {
        if (key== null || !dataStore.containsKey(key))
            return Collections.emptyList();

        return dataStore.get(key).entrySet().stream()
                .filter(entry -> !entry.getValue().isExpired(timestamp))
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + " : " + entry.getValue().value)
                .collect(Collectors.toList());
    }

    public List<String> scanByPrefixAt(String key, String prefix, int timestamp)
    {
        if (key == null || prefix == null || !dataStore.containsKey(key))
            return Collections.emptyList();

        return dataStore.get(key).entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .filter(entry -> !entry.getValue().isExpired(timestamp))
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + " : " + entry.getValue().value)
                .collect(Collectors.toList());

    }

    private int backup(int timestamp)
    {
        Map<String, Map<String, ValueWithTTL>> backup= new HashMap<>(); //Create a deep copy of current state, filtering expired entries

        int recordCount=0;
        for (Map.Entry<String,Map<String, ValueWithTTL>> recordEntry: dataStore.entrySet())
        {
            Map<String, ValueWithTTL> recordCopy=new HashMap<>();

            for (Map.Entry<String, ValueWithTTL> fieldEntry: recordEntry.getValue().entrySet())
            {
                if(!fieldEntry.getValue().isExpired(timestamp))
                {
                    recordCopy.put(fieldEntry.getKey(), fieldEntry.getValue());
                }
            }
            if (!recordCopy.isEmpty())
            {
                backup.put(recordEntry.getKey(), recordCopy);
                recordCount++;
            }
        }
        backupStore.put(timestamp, backup);
        return recordCount;
    }

    public void restore(int currentTimestamp, int timestampToRestore)
    {
        // Find the most recent backup not newer than timestampToRestore
        Map.Entry<Integer, Map<String, Map<String,ValueWithTTL>>> backupEntry=backupStore.floorEntry(timestampToRestore);

        if (backupEntry==null)
            throw new IllegalStateException("No Backup Available for restoration");

        int backupTimestamp= backupEntry.getKey();
        Map<String, Map<String, ValueWithTTL>> backup=backupEntry.getValue();

        dataStore.clear();

        for (Map.Entry<String, Map<String, ValueWithTTL>> recordEntry: backup.entrySet())
        {
            Map<String, ValueWithTTL> newRecord= new HashMap<>();
            for (Map.Entry<String, ValueWithTTL> fieldEntry: recordEntry.getValue().entrySet())
            {
                ValueWithTTL original=fieldEntry.getValue();
                int newExpiration = original.expirationTime == -1 ?
                        -1 : original.expirationTime - backupTimestamp +currentTimestamp;

                newRecord.put(fieldEntry.getKey(), new ValueWithTTL(original.value, newExpiration));

            }
            dataStore.put(recordEntry.getKey(), newRecord);

        }
    }

//    private final Map<String, Map<String, String>> dataStore;
//
//    public InMemoryDB()
//    {
//        this.dataStore= new HashMap<>();
//    }
//
//    public void set(String key, String field, String value)
//    {
//        if(key == null || field == null)
//            throw new IllegalArgumentException("Key or field cannot be null");
//
//        Map<String,String> record= dataStore.computeIfAbsent(key, k->new HashMap<>());
//        record.put(field,value);
//
//    }
//
//    public Optional<String> get(String key, String field)
//    {
//        if(key == null || field == null )
//            return Optional.empty();
//
//        if(!dataStore.containsKey(key))
//            return Optional.empty();
//
//        Map <String, String> record= dataStore.get(key);
//        return record.containsKey(field)?Optional.of(record.get(field)):Optional.empty();
//    }
//
//    public boolean delete(String key, String field)
//    {
//        if (key == null || field == null)
//            return false;
//
//        if (!dataStore.containsKey(key))
//            return false;
//
//        Map<String, String> record= dataStore.get(key);
//        boolean existed= record.remove(field) != null;
//
//        if (record.isEmpty())
//            dataStore.remove(key);
//
//        return existed;
//
//
//    }
//
//    public List<String> scan(String key)
//    {
//        if(key == null || !dataStore.containsKey(key))
//            return Collections.emptyList();
//
//        List<String> result= new ArrayList<>();
//        Map<String, String> record=dataStore.get(key);
//
//        List<String> sortedFields= new ArrayList<>(record.keySet());
//        Collections.sort(sortedFields);
//
//        for (String field: sortedFields)
//        {
//            result.add(field + ":"+ record.get(field));
//        }
//
//        return result;
//    }
//
//    public List<String> scanByPrefix(String key, String prefix)
//    {
//        if((key == null || !dataStore.containsKey(key)))
//           return Collections.emptyList();
//
//        List<String> result= new ArrayList<>();
//        Map<String, String> record= dataStore.get(key);
//
//        List<String> matchingFields= new ArrayList<>();
//        for(String field: record.keySet())
//        {
//            if(field.startsWith(prefix))
//                matchingFields.add(field);
//        }
//
//        Collections.sort(matchingFields);
//
//        for(String field: matchingFields)
//        {
//            result.add(field + ":" + record.get(field));
//        }
//        return result;
//    }

//

}
