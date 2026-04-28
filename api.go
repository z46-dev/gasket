package gasket

import "github.com/z46-dev/gomysql"

func NewClient(sqlDatabaseFilePath string) (client *Client, err error) {
	client = &Client{}

	if client.driver, err = gomysql.Begin(sqlDatabaseFilePath); err != nil {
		return
	}

	if err = client.register(); err != nil {
		return
	}

	return
}

func (c *Client) Close() (err error) {
	if c.driver != nil {
		err = c.driver.Close()
	}

	return
}

func (c *Client) register() (err error) {
	if c.tasksDB, err = gomysql.Register(c.driver, Task{}); err != nil {
		return
	}

	if c.taskSchedulesDB, err = gomysql.Register(c.driver, TaskSchedule{}); err != nil {
		return
	}

	if c.taskRetryPoliciesDB, err = gomysql.Register(c.driver, TaskRetryPolicy{}); err != nil {
		return
	}

	return
}

func (c *Client) GetActiveDatabaseFilePath() (filePath string) {
	if c.driver != nil {
		filePath = c.driver.GetFilePath()
	}

	return
}
