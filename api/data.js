import { Router } from 'express';
import {getAllData, getDataById, addData, updateData, deleteData, getUsersRecords, addDataProc} from '../db/mongodb.js';
let router = Router()

router.get('/', async (req, res) => {
    res.json( await getAllData() )
})

router.get('/users_records', async (req, res) => {
    res.json( await getUsersRecords() )
})

router.get('/:id', async (req, res) => {
    res.json( await getDataById(req.params.id) )
})

router.post('/procedure', async (req, res) => {
  const { Firstname, Surname } = req.body

  if (!Firstname || !Surname) {
    return res.status(400).json({ error: 'Missing data' })
  }

  try {
    await addDataProc(Firstname, Surname)
    res.status(201).json({
      message: 'procedure called'
    })
  } catch (err) {
    console.error(err)
    res.status(500).json({
      error: 'procedure error'
    })
  }
})


router.post('/', async (req, res) => {
    let [exist] = await getDataById(req.body.id)
    if( exist ) {
        res.status(409).json( {"error": "record already exists"});
    } else {
        let result = await addData(req.body);
        if(result)
            res.json(req.body);
        else
            res.status(500).json({"error": "unknown database error"})
    }
})

router.put('/:id', async (req, res) => {
  let [exist] = await getDataById(req.params.id)
  if (!exist) {
    res.status(404).json({ "error": "record not found" })
  } else {
    let result = await updateData(
      req.params.id,
      req.body.Firstname,
      req.body.Surname
    )

    if (result)
      res.json({ "message": "Record Updated" })
    else
      res.status(500).json({ "error": "unknown database error" })
  }
})

router.delete('/:id', async (req, res) => {
  let [exist] = await getDataById(req.params.id)
  if (!exist) {
    res.status(404).json({ "error": "record not found" })
  } else {
    let result = await deleteData(req.params.id)

    if (result)
      res.json({ "message": "Record Deleted" })
    else
      res.status(500).json({ "error": "unknown database error" })
  }
})

export default router;