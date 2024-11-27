# MobileTransactionFraudDetectionSystem
Cloud-oriented implementation of a mobile transaction fraud detection system

Here’s a summary of all the tables we have designed so far, their attributes, and how they relate to each other, along with the ER diagram structure and referential integrity constraints.

---

### **List of Tables**

#### 1. **SUBSCRIBER**
- **Attributes**:
  - `Subscriber_ID` (Primary Key)
  - `Name`
  - `Phone_Number`
  - `Email`
  - `Address`
  - `Join_Date`
  - `Device_ID` (Foreign Key referencing `DEVICE_INFO.Device_ID`)

#### 2. **MESSAGE**
- **Attributes**:
  - `Message_ID` (Primary Key)
  - `Sender_ID` (Foreign Key referencing `SUBSCRIBER.Subscriber_ID`)
  - `Receiver_ID` (Foreign Key referencing `SUBSCRIBER.Subscriber_ID`)
  - `Message_Type` (e.g., SMS, MMS)
  - `Content` (optional, encrypted or anonymized for privacy)
  - `Timestamp`
  - `Status` (e.g., Sent, Delivered, Failed)

#### 3. **TRANSACTION**
- **Attributes**:
  - `Transaction_ID` (Primary Key)
  - `Subscriber_ID` (Foreign Key referencing `SUBSCRIBER.Subscriber_ID`)
  - `Transaction_Type` (e.g., Recharge, Bill Payment)
  - `Amount`
  - `Timestamp`
  - `Payment_Method` (e.g., Credit Card, Mobile Money)

#### 4. **CALL_LOG**
- **Attributes**:
  - `Call_ID` (Primary Key)
  - `Subscriber_ID` (Foreign Key referencing `SUBSCRIBER.Subscriber_ID`)
  - `Phone_Number` (destination number)
  - `Call_Type` (e.g., Incoming, Outgoing, Missed)
  - `Call_Start`
  - `Call_End`
  - `Duration`
  - `Call_Status` (e.g., Completed, Failed)

#### 5. **SIM_INFO**
- **Attributes**:
  - `SIM_ID` (Primary Key)
  - `Subscriber_ID` (Foreign Key referencing `SUBSCRIBER.Subscriber_ID`)
  - `IMSI` (International Mobile Subscriber Identity)
  - `ICCID` (Integrated Circuit Card Identifier)
  - `Carrier`
  - `Activation_Date`
  - `Deactivation_Date` (nullable)

#### 6. **ISP_DATA_TRAFFIC**
- **Attributes**:
  - `Traffic_ID` (Primary Key)
  - `Subscriber_ID` (Foreign Key referencing `SUBSCRIBER.Subscriber_ID`)
  - `IP_Address`
  - `URL_Visited`
  - `Timestamp`
  - `Protocol` (e.g., HTTP, HTTPS)
  - `Data_Transferred` (volume in MB)
  - `Geo_Location`
  - `Traffic_Status` (e.g., Allowed, Blocked)

#### 7. **DEVICE_INFO**
- **Attributes**:
  - `Device_ID` (Primary Key)
  - `Subscriber_ID` (Foreign Key referencing `SUBSCRIBER.Subscriber_ID`)
  - `Device_Type` (e.g., Smartphone, Tablet)
  - `OS_Version`
  - `IMEI` (International Mobile Equipment Identity)
  - `Manufacturer`
  - `Model`

---

### **ER Diagram Relationships**

1. **SUBSCRIBER → MESSAGE**:
   - `SUBSCRIBER` is related to `MESSAGE` via `Sender_ID` and `Receiver_ID`.
   - Referential Integrity: `Sender_ID` and `Receiver_ID` in `MESSAGE` must exist as `Subscriber_ID` in `SUBSCRIBER`.

2. **SUBSCRIBER → TRANSACTION**:
   - `SUBSCRIBER` is related to `TRANSACTION` via `Subscriber_ID`.
   - Referential Integrity: `Subscriber_ID` in `TRANSACTION` must exist in `SUBSCRIBER`.

3. **SUBSCRIBER → CALL_LOG**:
   - `SUBSCRIBER` is related to `CALL_LOG` via `Subscriber_ID`.
   - Referential Integrity: `Subscriber_ID` in `CALL_LOG` must exist in `SUBSCRIBER`.

4. **SUBSCRIBER → SIM_INFO**:
   - `SUBSCRIBER` is related to `SIM_INFO` via `Subscriber_ID`.
   - Referential Integrity: `Subscriber_ID` in `SIM_INFO` must exist in `SUBSCRIBER`.

5. **SUBSCRIBER → ISP_DATA_TRAFFIC**:
   - `SUBSCRIBER` is related to `ISP_DATA_TRAFFIC` via `Subscriber_ID`.
   - Referential Integrity: `Subscriber_ID` in `ISP_DATA_TRAFFIC` must exist in `SUBSCRIBER`.

6. **SUBSCRIBER → DEVICE_INFO**:
   - `SUBSCRIBER` is related to `DEVICE_INFO` via `Subscriber_ID`.
   - Referential Integrity: `Subscriber_ID` in `DEVICE_INFO` must exist in `SUBSCRIBER`.

---

### **ER Diagram Overview**

#### Entities and Relationships:
1. **SUBSCRIBER**:
   - Central entity, connected to `MESSAGE`, `TRANSACTION`, `CALL_LOG`, `SIM_INFO`, `ISP_DATA_TRAFFIC`, and `DEVICE_INFO`.

2. **MESSAGE**:
   - Connected to `SUBSCRIBER` (Sender and Receiver).

3. **TRANSACTION**:
   - Connected to `SUBSCRIBER`.

4. **CALL_LOG**:
   - Connected to `SUBSCRIBER`.

5. **SIM_INFO**:
   - Connected to `SUBSCRIBER`.

6. **ISP_DATA_TRAFFIC**:
   - Connected to `SUBSCRIBER`.

7. **DEVICE_INFO**:
   - Connected to `SUBSCRIBER`.

---

### Referential Integrity Rules
1. **Foreign Keys**:
   - Every foreign key (`Subscriber_ID`, `Sender_ID`, `Receiver_ID`, `Device_ID`) must match a primary key in its respective parent table.

2. **Deletion Cascade Rules**:
   - If a `SUBSCRIBER` is deleted, associated data in dependent tables (`MESSAGE`, `TRANSACTION`, `CALL_LOG`, `SIM_INFO`, `ISP_DATA_TRAFFIC`, `DEVICE_INFO`) may either:
     - Be deleted (CASCADE).
     - Be retained but marked as orphaned (SET NULL).

3. **Null Constraints**:
   - Attributes such as `Sender_ID` and `Receiver_ID` in `MESSAGE` must not be null.
   - `Deactivation_Date` in `SIM_INFO` can be null if the SIM is active.

---

Would you like me to create a detailed textual or graphical representation of the ER diagram?
