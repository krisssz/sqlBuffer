import _mysql

class sqlBuffer:
	
	def __init__(self, DataBase, table, columns, length=1000):
		self.db = DataBase
		self.table = table
		self.columns = columns
		self.length = length
		self.count = 0
		self.query = []
		self.query.append("INSERT INTO %s (%s) VALUES " % (table, ', '.join(str(c) for c in columns)))
		return self
	
	def add(self, str):
		if self.count != 0:
			self.query.append(", " + str)
		else:
			self.query.append(str)
		self.count += 1
		if self.count == self.length:
			self.send()
		return self
		
	def send(self):
		self.db.query(''.join(self.query))
		self.__init__(self.db, self.table, self.columns, self.length)
		return self
		
	def close(self):
		if self.count != 0:
			self.send()
		self.db.commit()
		return self
