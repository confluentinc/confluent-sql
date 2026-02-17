# Implement a Flink statement result changelog compressor

Streaming, non-append-only Flink statement results come to us as a changelog which all clients will then need to process in similar ways. We need to write a conveient, centralized implementation of a changelog compressor, accessible and constructed by the cursor via a new method (`.changelog_compressor()`) as an extension to DBAPI.

The compressor will manage a logical result set of rows that changes over time. Rows will be added, revised, and / or deleted over time as the consumed changelog events come in.

Consult and research before proceeding:

- RawChangelogProcessor + Cursor, and the ChangeloggedRow rows that such a cursor's fetchmany() will return.
- Class Op describing changelog stream events. UPDATE*BEFORE, UPDATE_AFTER, and DELETE events essentially \_mutate* the current result set.
- Statement.traits.upsert_columns: list of the zero-based indexes of the columns within Statement.schema.columns which will act as a (possibly compound) unique key to identify rows from the statement. If the upsert_columns list is nonempty, we can use those fields to make a tuple to act as a key to find any such record so that when an UPDATE_BEFORE, UPDATE_AFTER, or DELETE operation is consumed, we can find it quickly (probably using a dict). But when upsert_columns is empty, there's no globally unique key for this result set, and any UPDATE_BEFORE, UPDATE_AFTER, or DELETE operation will need to find the most recent entire row which matches and then act accordingly.

First off, I want to focus on what kind of overall external API the compressor(s) should have which would be ergonomic for client code to use. I want the user to obtain the compressor from a streaming/non-append-only cursor, and then use the compressor for all subsequent interactions with the result set. The compressor will retain a reference to the cursor. The compressor should internally only interact with the cursor's public API and to _never_ reach into the cursor's private fields. Probably mainly through driving cursor.fetchmany(N).

I ... think that the closest thing to being able to identify points in time when we could let the client code interact with the 'current' set of rows as produced by the sum of the changelog operations at a specific point will be when cursor.fetchmany(N) returns an empty list. At those points we could let the user get a deep copy of the result rows. Then when they invoke an operation on the compressor to let it drain more changelog rows from the cursor, it will do so repeatedly until the cursor.fetchmany(N) call returns empty again.

We will need a new module, changelog_compressor.py to hold the new code. I think we might then need an ABC describing the overall API / operations offered by the compressor to end-user code. Then probably two main intermediate subclasses, one for when the statement has upsert_columns, and a second one when upsert_columns is empty. Then perhaps also two mixin classes based on if the cursor was created with as_dict=True or not. Ultimately then four concrete compressor classes:

    * One for has upsert columns, as_dict == False. The result set managed and exposed to the user will be a list of tuples, and can be implemented with a dict with keys based on the row fields indicated by the upsert columns. Now with modern python dicts being ordered, a single internal dict may be all that we need for internal data storage.

    * One for has upsert columns, as_dict == True. The result set managed and exposed to the user will be a list of dicts, not tuples.

    * One for does not have upsert columns, as_dict == False. Here we will manage a list of tuples, period. When we have to mutate or delete a previously encountered row, we will find the most recent equal row by scanning backwards through our list, then operating on it.

    * One for does not have upsert columns, as_dict == True. Same overall behavior as prior, but this time the row type will be dicts, not tuples.

Would probably want to make the compressor ABC be generic over ResultTupleOrDict (from changelog.py) with the concrete subclasses making use of either StatementResultTuple or StrAnyDict.
