import { MongoClient } from "mongodb";
import "dotenv/config";

const uri = process.env.MONGO_URI;
const client = new MongoClient(uri);

await client.connect();

const db = client.db(process.env.MONGO_DB);

const dataCollection = db.collection("data");
const usersCollection = db.collection("users");

const logonUsers = new Map();

const findUser = async (username) => {
  const user = await usersCollection.findOne({ username });
  return user ? [user] : [];
};

const getAllData = async () => {
  return await dataCollection.find().toArray();
};

const getDataById = async (id) => {
  const record = await dataCollection.findOne({ id });
  return record ? [record] : [];
};

const addData = async ({ id, Firstname, Surname, userid }) => {
  const result = await dataCollection.insertOne({
    id,
    Firstname,
    Surname,
    userid
  });
  return result.acknowledged;
};

const updateData = async (id, Firstname, Surname) => {
  const result = await dataCollection.updateOne(
    { id },
    { $set: { Firstname, Surname } }
  );
  return result.modifiedCount > 0;
};

const deleteData = async (id) => {
  const result = await dataCollection.deleteOne({ id });
  return result.deletedCount > 0;
};

const addDataProc = async (Firstname, Surname) => {

  const randomUser = await usersCollection.aggregate([
    { $sample: { size: 1 } }
  ]).toArray();

  const randomUserId = randomUser[0].username;

  const maxRecord = await dataCollection
    .find()
    .sort({ id: -1 })
    .limit(1)
    .toArray();

  const newId = maxRecord.length > 0
    ? (parseInt(maxRecord[0].id) + 1).toString()
    : "1";

  await dataCollection.insertOne({
    id: newId,
    Firstname,
    Surname,
    userid: randomUserId
  });

  return true;
};

const getUsersRecords = async () => {
  return await dataCollection.aggregate([
    {
      $group: {
        _id: "$userid",
        total: { $sum: 1 }
      }
    }
  ]).toArray();
};

export {
  addDataProc,
  updateData,
  deleteData,
  addData,
  findUser,
  getAllData,
  getDataById,
  logonUsers,
  getUsersRecords
};
