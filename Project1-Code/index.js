const { MongoClient , ObjectId} = require('mongodb');

const uri = "mongodb://localhost:27017/ParkinsonsData";
const sourceCollectionName = 'Samples';
const statsCollectionName = 'statistics_ParkinsonsData';
const frequencyCollectionName = 'frequency_ParkinsonsData';
const statistics1CollectionName = `statistics1_ParkinsonsDataset`;
const statistics2CollectionName = `statistics2_ParkinsonsDataset`;
const embCollectionName = `emb_ParkinsonsDataset`;
const emb2CollectionName = `emb2_ParkinsonsDataset`;

const continuousVars = ['subject#', 'age', 'test_time', 'motor_UPDRS', 'total_UPDRS', 'Jitter(%)', 'Jitter(Abs)', 'Jitter:RAP', 'Jitter:PPQ5', 'Jitter:DDP',
'Shimmer', 'Shimmer(dB)', 'Shimmer:APQ3', 'Shimmer:APQ5', 'Shimmer:APQ11', 'Shimmer:DDA', 'NHR', 'HNR', 'RPDE', 'DFA', 'PPE'];
const categoricalVars = ['sex'];

async function processData() {
  const client = new MongoClient(uri);

  try {
    //Here we're connecting to mongo using uri above, to ParkinsonsData database
    await client.connect();
    console.log('Connection to DB SUCCESS!');

    const database = client.db();
    //Creating collections inside the DB to be used for the future tasks
    const sourceCollection = database.collection(sourceCollectionName);
    const statsCollection = database.collection(statsCollectionName);
    const freqCollection = database.collection(frequencyCollectionName);
    const statistics1Collection = database.collection(statistics1CollectionName);
    const statistics2Collection = database.collection(statistics2CollectionName);
    const embCollection = database.collection(embCollectionName);
    const emb2Collection = database.collection(emb2CollectionName);

    //resetting data incase we wanted to re-execute the script (so we wont have duplicates)
    await statsCollection.deleteMany({});
    await freqCollection.deleteMany({});
    await statistics1Collection.deleteMany({});
    await statistics2Collection.deleteMany({});
    await embCollection.deleteMany({});
    await emb2Collection.deleteMany({});

    //Task 1: we replace all empty values of Continuous attributes with -1, and for Categorical attributes with 'Empty' string
    let updateOps = {};
    continuousVars.forEach(field => {
      updateOps[field] = { $ifNull: [`$${field}`, -1] };
    });
    categoricalVars.forEach(field => {
      updateOps[field] = { $ifNull: [`$${field}`, "empty"] };
    });

    await sourceCollection.updateMany({}, [{ $set: updateOps }]);


    //Task 2: we calculate mean, standard deviation, and number of elements, for this the aggragate function was used (1 pipeline) and the result was grouped using _id
    //documentation for aggragate function can be found on : https://www.mongodb.com/docs/manual/aggregation/
    //$group function was used according to https://www.mongodb.com/docs/manual/aggregation/#:~:text=%24group%20stage.-,The%20%24group%20stage%3A,-Groups%20the%20remaining
    //$avg function was used according to https://www.mongodb.com/docs/manual/reference/operator/aggregation/avg/#:~:text=%22%24item%22%2C-,avgAmount%3A%20%7B%20%24avg%3A,-%7B%20%24multiply%3A
    //$stdDevSamp was used according to https://www.mongodb.com/docs/manual/reference/operator/aggregation/stdDevSamp/#:~:text=ageStdDev%3A%20%7B-,%24stdDevSamp%3A%20%22%24age%22,-%7D%20%7D%20%7D
    //$sum was used according to https://www.mongodb.com/docs/manual/reference/operator/aggregation/sum/#:~:text=quantity%22%20%5D%20%7D%20%7D%2C-,count%3A%20%7B%20%24sum%3A%201%20%7D,-%7D

    var idCounterContinuous = 0;
    for (const field of continuousVars) {
      idCounterContinuous++;
      const result = await sourceCollection.aggregate([
        { $group: {
            _id: idCounterContinuous,
            mean: { $avg: `$${field}` },
            stddev: { $stdDevSamp: `$${field}` },
            count: { $sum: 1 }
          }
        }
      ]).toArray();

      if (result.length > 0) {
        await statsCollection.insertOne({
          Variable: field,
          statistics: result[0]
        });
      }
    }

    console.log('Statistics calculated and inserted into a new collection');

    //Task 3: Calculate frequency using $inc, first we go through all the data row by row(.next()) and for each row we use $inc to the related field, if field doesnt exist, we create it and fill them inside frequency document.
    //$inc was used according to https://www.mongodb.com/docs/manual/reference/operator/update/inc/#:~:text=%24inc%3A%20%7B%20quantity%3A%20%2D2%2C%20%22metrics.orders%22%3A%201%20%7D
    //updateOne was used according to https://www.geeksforgeeks.org/mongodb-updateone-method-db-collection-updateone/#:~:text=db.collection.updateOne(%3Cfilter%3E%2C%20%3Cupdate%3E%2C%20%7B%0A%C2%A0%20%C2%A0upsert%3A%20%3Cboolean%3E%2C

    const cursor = sourceCollection.find();
    while (await cursor.hasNext()) {
      const doc = await cursor.next();
    
      for (const field of categoricalVars) {
        if (doc[field] !== undefined && doc[field] !== null) { // if null then we skip it (just incase so i dont get random errors)
          const updateField = `${field}.${doc[field]}`;
    
          await freqCollection.updateOne(
            { _id: field },
            { $inc: { [updateField]: 1 } },
            { upsert: true } // Ensures that a new document is created if it doesn't exist
          );
        }
      }
    }
    console.log('Frequency counts calculated and inserted into new collection');



    //Task 4: Create 2 new documents, and fill the first one with the rows which are lower than the mean and the second one with higher.

    //itirate over all the attributes
    for (const field of continuousVars) {
      // get the mean from the statistics table
      const result = await statsCollection.findOne({ Variable: field });
      const avgValue = result.statistics.mean;
      // Initialize documents in statistics1Collection and statistics2Collection
      await statistics1Collection.insertOne({
        _id: new ObjectId(),
        variable: field,
        count: 0,
        mean_value: avgValue,
        values: []
      });

      await statistics2Collection.insertOne({
        _id: new ObjectId(),
        variable: field,
        count: 0,
        mean_value: avgValue,
        values: []
      });

      // itirate over all the rows
      const cursor = sourceCollection.find();
      while (await cursor.hasNext()) {
        const document = await cursor.next();
          if (document[field] !== undefined) {
              if (document[field] <= avgValue) {
                  // value less than mean
                  await statistics1Collection.updateOne(
                      { variable: field },
                      {
                      $inc: { count: 1 },
                      $push: { values: document[field] }
                      }
                  );
              } else {
                  // value higher than mean
                  await statistics2Collection.updateOne(
                      { variable: field },
                      {
                      $inc: { count: 1 },
                      $push: { values: document[field] }
                      }
                  );
              }
          }
      };
  }
  console.log('Documents divided and inserted into statistics1 and statistics2 collections');

  //Task 5: clone the first document and add to it the frequencies for the categorical values and insert it into new collection (emb).
  // ... (spread operator in js) to clone the doc

  const documents = await sourceCollection.find().toArray();

  for (const doc of documents) {
      let embedDoc = { ...doc };

      for (const catVar of categoricalVars) {
          // Retrieve frequency data for the categorical variable
          const freqData = await freqCollection.findOne({ _id: catVar });
          
          if (freqData) {
              delete freqData._id; // Remove the _id field from freqData
              embedDoc[`${catVar}_frequencies`] = freqData; //and just add the frequency value
          }
      }

      await embCollection.insertOne(embedDoc);
  }

  console.log('Task 5 completed: Documents with embedded categorical values inserted into new collection (emb_ParkinsonsDataset).');


  //Task 6:clone the first document and add to it the mean and standard deviation and count for the continues values and insert it into new collection (emb2).
  // ... (spread operator in js) to clone the doc


  const baseDocuments = await sourceCollection.find().toArray();

    for (const baseDoc of baseDocuments) {
        let embeddedDoc = { ...baseDoc };

        for (const contVar of continuousVars) {
          // Retrieve data for the continous variable
            const statsData = await statsCollection.findOne({ Variable: contVar });

            if (statsData) {
                embeddedDoc[`${contVar}_stats`] = {
                    average: statsData.statistics.mean, //add the mean as average
                    standardDeviation: statsData.statistics.stddev, //add the stddev as standardDeviation
                    nonMissingCount: statsData.statistics.count //add the count as nonMissingCount (this should be equal to total amount if data is not missing)
                };
            }
        }
        await emb2Collection.insertOne(embeddedDoc);
    }

    console.log('Task 6 completed: Documents with embedded continuous variable statistics inserted into new collection (emb2_ParkinsonsDataset).');


  //Task 7: Itirate over emb2 collection, and check if the standard deviation is 10% bigger then the mean
  //first we get the collection, and the for every continous attribute we have, we read the value of 1.1*mean and we compare it to SD, if its bigger, we use $Set to add another attribute to the document which is equal to true, if not its equal to false.
  //$Set was used according to https://www.mongodb.com/docs/manual/reference/operator/update/set/#:~:text=the%20details%20document%3A-,db.products.updateOne(,),-After%20updating%2C%20the

  const documentsToUpdate = await emb2Collection.find().toArray();

    for (const doc of documentsToUpdate) {
        for (const contVar of continuousVars) {
            const statsKey = `${contVar}_stats`;
            if (doc[statsKey] && doc[statsKey].standardDeviation > 1.1 * doc[statsKey].average) {
                await emb2Collection.updateOne({ _id: doc._id }, { $set: {ExcedingValue : {value : true}} });
            }
            else{
              await emb2Collection.updateOne({ _id: doc._id }, { $set: {ExcedingValue : {value : false}} });
            }
        }
    }

    console.log('Task 7 completed: Documents updated with statistics exceeding threshold.');

    //Task 8: Creating indexs for the collection, in this case i created index for age and for sex attributes
    //createIndex was used according to https://www.mongodb.com/docs/manual/reference/method/db.collection.createIndex/#:~:text=Create%20an-,Index%20on%20a%20Multiple%20Fields,-The%20following%20example
    const indexField1 = "age"; //descending order
    const indexField2 = "sex"; //ascending order

    await sourceCollection.createIndex({ [indexField1]: -1, [indexField2]: 1 });

    console.log('Task 8 completed: Compound index created on the original collection.');
  } catch (error) {
    console.error("An error occurred:", error);
  } finally {
    if (client && client.isConnected()) {
      await client.close();
      console.log('Connection closed');
    }
    
  }
}
processData().catch(console.error);
