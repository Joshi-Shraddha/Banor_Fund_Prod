funds
============


Description
-----------
Using this feature - user can place an order for Buy or Sell Funds. User can perform this action on following funds:

1) Italy Long short
2) NorthAmerica Long short
3) GREATERCHINA Long short
4) EUROPEANVALUE Long short
5) CHIRON
6) EUROBOND
7) NEWFRONTIERS
8) ROSEMARY
9) ASIANALPHA
10) HIGHFOCUS
11) MEAOPPORTUNITIES
12) LEVEURO
13) ASIMOCCO
14) RAFFAELLO


**The SQL tables which are updated:**

- OSUSR_38P_ORDERS
- Funds tabel (i.e OSUSR_38P_NORTHAMERICALS,OSUSR_38P_ITALYLS and so on)
- OSUSR_SKP_ORDERCHANGES


How to run
----------
This is backend point to insert new order. This can be done by clicking on new position button from the funds tab in the Banor system.



The Process
-----------------------------

The steps for inserting orders are:

1. Click on Funds tab from banor system.
2. Select (click on) the fund for which you want to place order.
3. Click on new position button. New position pop-up get open.
4. Select type (Buy/shell)
5. Select security type.
6. User should provide Ticker,Weight Target,Selection Broker,Quantity, Multiplier,limit, urgency and expiry.
7. click on send button.



Technical Documentation
-----------------------



.. automodule:: funds
   :members:
   :undoc-members:
   :show-inheritance:
