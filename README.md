# certificate_checker 
[![codecov](https://codecov.io/gh/confirmationbias616/certificate_checker/branch/master/graph/badge.svg)](https://codecov.io/gh/confirmationbias616/certificate_checker)
[![CircleCI](https://circleci.com/gh/confirmationbias616/certificate_checker.svg?style=svg)](https://circleci.com/gh/confirmationbias616/certificate_checker)

<P class="blocktext">
    The idea behind this tool is to minimize subcontractors' average wait
  time for receiving their hard-earned 10% holdback payemnts. This is
    acheived by monitoring all known commercial news websites for
    postings of CSP (Certificate of Substantial Completion) and notify
    involved subcontractors. Applicable to all construction projects in
    Ontario, Canada.
</P>
<br>
<P class="blocktext">
    The '<b>HBR</b>' stands for <b>H</b>old<b>b</b>ack <b>R</b>elease. The
    word 'Bot' intends to communicate that the matching algorithm in the backend
    makes use of machine learning technology. This means that the service will
    automatically improve with time as it sees more data come through. For more
    details on the tech stack, see last paragraph.
</P>
<br>
<P class="blocktext">
    To get started, simply enter your project's info in the 
    <a href="https://hbr-bot.ca">home page</a> form
    and let HBR Bot guide you through the process. To learn more about the Lien
    Construction Act and holdback releases in general, visit
    <a href="https://canada.constructconnect.com/dcn/construction-act">this link<a>.</a>
</P>
<br>
<P class="blocktext">
    As for the <b>tech stack</b>, HBR Bot is written in Python and SQL. It essentially
    consists of a machine learning model wrapped in a web app. At a high level,
    the type of data science problem being solved here can be classified as
    <a href='http://www.datacommunitydc.org/blog/2013/08/entity-resolution-for-big-data'>
        Entity Resoultion</a>.
    The machine learning model is a Scikit-Learn Random Forest and it makes use of
    an active style of learning with human-in-the-loop validation via user feedback
    from the web app. Due to the class imbalance issue of an initially small amount
    of confirmed positive matches, the ML training process makes use of the SMOTE
    algorithm. The data wrangling step utilizes the fuzzywuzzy package, which
    applies fuzzy logic to all string mtaching. The web app is built with Flask and is
    hosted on PythonAnywhere. The database is SQLite3. The code base is version
    controlled with Git and hosted on GitHub. It is also automatically tested and
    continuously integrated on CircleCI.
</P>
