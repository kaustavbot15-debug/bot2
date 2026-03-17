import pymongo
import time
import motor.motor_asyncio
from bson.objectid import ObjectId
from bson.errors import InvalidId
from FileStream.server.exceptions import FIleNotFound
import asyncio
import random

class Database:
    def __init__(self, uri, database_name):
        if isinstance(uri, str):
            self._uris = [uri]
        else:
            self._uris = uri

        self.clients = [motor.motor_asyncio.AsyncIOMotorClient(u) for u in self._uris]
        self.dbs = [client[database_name] for client in self.clients]
        self.cols = [db.users for db in self.dbs]
        self.blacks = [db.blacklist for db in self.dbs]
        self.files = [db.file for db in self.dbs]

#---------------------[ NEW USER ]---------------------#
    def new_user(self, id):
        return dict(
            id=id,
            join_date=time.time(),
            Links=0
        )

# ---------------------[ ADD USER ]---------------------#
    async def add_user(self, id):
        if await self.get_user(id):
            return
        user = self.new_user(id)
        await random.choice(self.cols).insert_one(user)

# ---------------------[ GET USER ]---------------------#
    async def get_user(self, id):
        tasks = [col.find_one({'id': int(id)}) for col in self.cols]
        results = await asyncio.gather(*tasks)
        for user in results:
            if user:
                return user
        return None

# ---------------------[ CHECK USER ]---------------------#
    async def total_users_count(self):
        tasks = [col.count_documents({}) for col in self.cols]
        counts = await asyncio.gather(*tasks)
        return sum(counts)

    async def get_all_users(self):
        async def generator():
            for col in self.cols:
                async for user in col.find({}):
                    yield user
        return generator()

# ---------------------[ REMOVE USER ]---------------------#
    async def delete_user(self, user_id):
        tasks = [col.delete_many({'id': int(user_id)}) for col in self.cols]
        await asyncio.gather(*tasks)

# ---------------------[ BAN, UNBAN USER ]---------------------#
    def black_user(self, id):
        return dict(
            id=id,
            ban_date=time.time()
        )

    async def ban_user(self, id):
        if await self.is_user_banned(id):
            return
        user = self.black_user(id)
        await random.choice(self.blacks).insert_one(user)

    async def unban_user(self, id):
        tasks = [black.delete_one({'id': int(id)}) for black in self.blacks]
        await asyncio.gather(*tasks)

    async def is_user_banned(self, id):
        tasks = [black.find_one({'id': int(id)}) for black in self.blacks]
        results = await asyncio.gather(*tasks)
        return any(results)

    async def total_banned_users_count(self):
        tasks = [black.count_documents({}) for black in self.blacks]
        counts = await asyncio.gather(*tasks)
        return sum(counts)
        
# ---------------------[ ADD FILE TO DB ]---------------------#
    async def add_file(self, file_info):
        file_info["time"] = time.time()
        fetch_old = await self.get_file_by_fileuniqueid(file_info["user_id"], file_info["file_unique_id"])
        if fetch_old:
            return fetch_old["_id"]
        await self.count_links(file_info["user_id"], "+")
        return (await random.choice(self.files).insert_one(file_info)).inserted_id

# ---------------------[ FIND FILE IN DB ]---------------------#
    async def find_files(self, user_id, range):
        offset = range[0] - 1
        limit = range[1] - range[0] + 1
        need = offset + limit

        tasks = []
        for col in self.files:
            t = col.find({"user_id": user_id}).sort('_id', pymongo.DESCENDING).limit(need).to_list(length=need)
            tasks.append(t)

        results = await asyncio.gather(*tasks)
        all_docs = []
        for r in results:
            all_docs.extend(r)

        all_docs.sort(key=lambda x: x['_id'], reverse=True)
        sliced = all_docs[offset : offset + limit]

        count_tasks = [col.count_documents({"user_id": user_id}) for col in self.files]
        counts = await asyncio.gather(*count_tasks)
        total_files = sum(counts)

        async def generator():
            for doc in sliced:
                yield doc

        return generator(), total_files

    async def get_file(self, _id):
        try:
            oid = ObjectId(_id)
        except InvalidId:
            raise FIleNotFound

        tasks = [col.find_one({"_id": oid}) for col in self.files]
        results = await asyncio.gather(*tasks)
        for file_info in results:
            if file_info:
                return file_info
        raise FIleNotFound
    
    async def get_file_by_fileuniqueid(self, id, file_unique_id, many=False):
        if many:
            async def generator():
                for col in self.files:
                    async for f in col.find({"file_unique_id": file_unique_id}):
                        yield f
            return generator()
        else:
            tasks = [col.find_one({"user_id": id, "file_unique_id": file_unique_id}) for col in self.files]
            results = await asyncio.gather(*tasks)
            for file_info in results:
                if file_info:
                    return file_info
            return False

# ---------------------[ TOTAL FILES ]---------------------#
    async def total_files(self, id=None):
        if id:
            tasks = [col.count_documents({"user_id": id}) for col in self.files]
        else:
            tasks = [col.count_documents({}) for col in self.files]
        counts = await asyncio.gather(*tasks)
        return sum(counts)

# ---------------------[ DELETE FILES ]---------------------#
    async def delete_one_file(self, _id):
        try:
             oid = ObjectId(_id)
        except InvalidId:
             return
        tasks = [col.delete_one({'_id': oid}) for col in self.files]
        await asyncio.gather(*tasks)

# ---------------------[ UPDATE FILES ]---------------------#
    async def update_file_ids(self, _id, file_ids: dict):
        try:
             oid = ObjectId(_id)
        except InvalidId:
             return
        tasks = [col.update_one({"_id": oid}, {"$set": {"file_ids": file_ids}}) for col in self.files]
        await asyncio.gather(*tasks)

# ---------------------[ PAID SYS ]---------------------#
#     async def link_available(self, id):
#         user = await self.col.find_one({"id": id})
#         if user.get("Plan") == "Plus":
#             return "Plus"
#         elif user.get("Plan") == "Free":
#             files = await self.file.count_documents({"user_id": id})
#             if files < 11:
#                 return True
#             return False
        
    async def count_links(self, id, operation: str):
        if operation == "-":
            tasks = [col.update_one({"id": id}, {"$inc": {"Links": -1}}) for col in self.cols]
            await asyncio.gather(*tasks)
        elif operation == "+":
            tasks = [col.update_one({"id": id}, {"$inc": {"Links": 1}}) for col in self.cols]
            await asyncio.gather(*tasks)
